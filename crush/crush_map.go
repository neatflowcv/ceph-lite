package crush

import "fmt"

// CrushMap은 CRUSH 맵의 구성 요소를 나타냅니다.
type CrushMap struct {
	Devices map[string]Device
	Buckets map[string]Bucket
	Rules   map[string]Rule
	RootID  int // 예: -1은 default root bucket
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
