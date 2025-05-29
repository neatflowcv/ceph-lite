package crush

import (
	"crypto/sha1"
	"encoding/hex"
	"fmt"
	"sort"
	"strconv"
)

// CrushMap은 CRUSH 맵의 구성 요소를 나타냅니다.
type CrushMap struct {
	Devices map[string]Device
	Buckets map[string]Bucket
	Rules   map[string]Rule
	RootID  int // 예: -1은 default root bucket
}

// Device는 OSD(Object Storage Device)를 나타냅니다.
type Device struct {
	Weight float64
	// 실제 Ceph에서는 더 많은 속성(예: class)이 있을 수 있습니다.
}

// Bucket은 CRUSH 맵의 계층 구조 내의 컨테이너를 나타냅니다.
type Bucket struct {
	ID        int
	Type      string   // 예: "host", "rack", "root"
	Algorithm string   // 예: "straw", "straw2"
	Items     []string // 이 버킷에 포함된 하위 아이템(OSD 또는 다른 버킷 이름)
}

// Rule은 데이터 복제 또는 이레이저 코딩 정책을 정의합니다.
type Rule struct {
	Steps []RuleStep
}

// RuleStep은 CRUSH 규칙 내의 단일 작업을 나타냅니다.
type RuleStep struct {
	Op    string // 예: "take", "chooseleaf", "emit"
	Item  string // 'take' 오퍼레이션의 시작 아이템
	Num   int    // 'chooseleaf' 오퍼레이션에서 선택할 아이템 수
	Type  string // 'chooseleaf' 오퍼레이션에서 고려할 실패 도메인 타입 (예: "rack", "host")
	Class string // 'take' 오퍼레이션에서 필터링할 OSD 클래스 (예: "hdd")
}

// NewCrushMap은 새로운 CrushMap 인스턴스를 초기화합니다.
func NewCrushMap() *CrushMap {
	return &CrushMap{
		Devices: make(map[string]Device),
		Buckets: make(map[string]Bucket),
		Rules:   make(map[string]Rule),
		RootID:  -1,
	}
}

func (cm *CrushMap) AddDevice(osdID int, weight float64) {
	cm.Devices[fmt.Sprintf("osd.%d", osdID)] = Device{Weight: weight}
}

func (cm *CrushMap) AddBucket(bucketName, bucketType, algorithm string, items []string) {
	cm.Buckets[bucketName] = Bucket{
		ID:        -(len(cm.Buckets) + 1), // 음수 ID는 내부적으로 버킷을 식별
		Type:      bucketType,
		Algorithm: algorithm,
		Items:     items,
	}
}

func (cm *CrushMap) AddRule(ruleName string, steps []RuleStep) {
	cm.Rules[ruleName] = Rule{Steps: steps}
}

func (cm *CrushMap) GetBucketItems(bucketName string) ([]string, bool) {
	bucket, ok := cm.Buckets[bucketName]
	if !ok {
		return nil, false
	}
	return bucket.Items, true
}

func (cm *CrushMap) GetDeviceWeight(deviceName string) float64 {
	dev, ok := cm.Devices[deviceName]
	if !ok {
		return 0.0
	}
	return dev.Weight
}

// ---------------------------------------------------------------
// CRUSH 해싱 함수 (간소화된 예시)
// 실제 CRUSH는 더 복잡한 해싱 및 난수 생성 기법을 사용합니다.
func crushHash(pgID int, bucketItemID int, retryCount int) int {
	seed := fmt.Sprintf("%d-%d-%d", pgID, bucketItemID, retryCount)
	h := sha1.New()
	h.Write([]byte(seed))
	hashBytes := h.Sum(nil)
	hexHash := hex.EncodeToString(hashBytes)

	// Go의 strconv.ParseInt는 base 16 (hex) 문자열을 파싱합니다.
	// 실제 CRUSH는 이보다 더 정교한 방법으로 해시 값을 사용합니다.
	val, _ := strconv.ParseInt(hexHash[:8], 16, 64) // 앞 8자리만 사용 예시
	return int(val)
}

// ---------------------------------------------------------------
// CRUSH 선택 로직의 핵심 (매우 간소화된 straw 알고리즘 예시)
// 실제 straw 알고리즘은 훨씬 더 복잡한 "강도" 계산 로직을 포함합니다.
func crushChooseStraw(bucketItems []string, pgID int, numToChoose int, cm *CrushMap, failureDomainType string) []string {
	selectedOSDs := []string{}

	// 편의를 위해 아이템을 복사하여 사용
	availableItems := make([]string, len(bucketItems))
	copy(availableItems, bucketItems)

	for i := 0; i < numToChoose; i++ {
		if len(availableItems) == 0 {
			break
		}

		bestItem := ""
		minHashVal := int(^uint(0) >> 1) // Go의 최대 정수 값

		for _, itemName := range availableItems {
			// itemName은 'osd.0' 또는 'host_myhost'와 같은 이름
			itemID := 0 // 실제로는 crush_map에서 itemName에 해당하는 id를 찾아야 함

			if osdPrefix := "osd."; len(itemName) > len(osdPrefix) && itemName[:len(osdPrefix)] == osdPrefix {
				idStr := itemName[len(osdPrefix):]
				id, err := strconv.Atoi(idStr)
				if err == nil {
					itemID = id
				}
			} else { // 버킷일 경우 해당 버킷의 내부 ID
				if bucket, ok := cm.Buckets[itemName]; ok {
					itemID = bucket.ID
				}
			}

			// 실제 CRUSH는 반복적인 해싱으로 최적의 아이템을 찾습니다.
			currentHashVal := crushHash(pgID, itemID, i) // i는 재시도 횟수 역할

			if currentHashVal < minHashVal {
				minHashVal = currentHashVal
				bestItem = itemName
			}
		}

		if bestItem != "" {
			selectedOSDs = append(selectedOSDs, bestItem)
			// 선택된 아이템을 availableItems에서 제거 (다음 선택에서 중복 방지)
			for j, item := range availableItems {
				if item == bestItem {
					availableItems = append(availableItems[:j], availableItems[j+1:]...)
					break
				}
			}

			// 만약 bestItem이 버킷이라면, 해당 버킷 내의 OSD를 재귀적으로 선택 (단일 OSD 선택 예시)
			if !isOSD(bestItem) {
				subOSDs := crushChooseStraw(cm.Buckets[bestItem].Items, pgID, 1, cm, failureDomainType)
				if len(subOSDs) > 0 {
					// 버킷 자체는 최종 OSD 목록에 포함되지 않으므로 제거하고, 하위 OSD 추가
					selectedOSDs = selectedOSDs[:len(selectedOSDs)-1]
					selectedOSDs = append(selectedOSDs, subOSDs...)
				}
			}
		}
	}

	// 실제 CRUSH는 failureDomainType을 사용하여 선택된 OSD들이 해당 도메인을 벗어나도록 보장합니다.
	// 예를 들어, 3개의 복제본이 모두 같은 랙에 선택되었다면, 다른 랙의 OSD를 다시 선택하는 로직이 추가됩니다.
	// 이 의사 코드에서는 이 복잡한 로직을 생략합니다.

	return selectedOSDs
}

// isOSD는 문자열이 OSD 이름 형식인지 확인합니다.
func isOSD(name string) bool {
	return len(name) > 4 && name[:4] == "osd."
}

// ---------------------------------------------------------------
// CRUSH 규칙 실행 함수 (간소화)
func CrushDoRule(pgID int, cm *CrushMap, ruleName string) ([]string, error) {
	rule, ok := cm.Rules[ruleName]
	if !ok {
		return nil, fmt.Errorf("Rule '%s' not found", ruleName)
	}

	selectedOSDs := []string{}
	currentItems := []string{} // 현재 처리 중인 아이템 목록 (OSD 또는 버킷)

	for _, step := range rule.Steps {
		switch step.Op {
		case "take":
			rootBucketName := step.Item // 예: "default"
			items, exists := cm.GetBucketItems(rootBucketName)
			if !exists {
				return nil, fmt.Errorf("Root bucket '%s' not found for rule step", rootBucketName)
			}
			currentItems = items

			// 클래스 필터링 (예: class hdd)
			if step.Class != "" {
				filteredItems := []string{}
				for _, item := range currentItems {
					if isOSD(item) {
						// 실제 OSD에는 class 정보가 태깅되어 있을 것입니다.
						// 여기서는 모든 OSD가 해당 클래스라고 가정합니다.
						filteredItems = append(filteredItems, item)
					}
				}
				currentItems = filteredItems
			}

		case "chooseleaf": // chooseleaf는 leaf 노드(OSD)를 선택
			numToChoose := step.Num
			failureDomainType := step.Type // 예: "rack", "host"

			chosenItems := crushChooseStraw(currentItems, pgID, numToChoose, cm, failureDomainType)
			selectedOSDs = append(selectedOSDs, chosenItems...)
			currentItems = chosenItems // 다음 스텝을 위해 현재 선택된 아이템을 업데이트

		case "emit":
			// emit은 현재까지 선택된 OSD들을 최종 결과로 내보냅니다.
			// 이 의사 코드에서는 emit을 명시적으로 처리하지 않고 최종 selectedOSDs를 반환합니다.
			// 실제 CRUSH는 emit 후 새로운 선택 라운드를 시작할 수 있습니다.
			// for range over selectedOSDs, remove from availableItems and repeat
			// 이 예제에서는 단순화를 위해 'emit' 오퍼레이션에서 추가적인 로직을 수행하지 않습니다.
		}
	}

	// 최종 OSD 목록을 정렬 (선택의 결정론적 순서를 위해 중요)
	sort.Strings(selectedOSDs)
	return selectedOSDs, nil
}
