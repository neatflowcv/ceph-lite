package crush

// Bucket은 CRUSH 맵의 계층 구조 내의 컨테이너를 나타냅니다.
type Bucket struct {
	ID        int
	Type      string   // 예: "host", "rack", "root"
	Algorithm string   // 예: "straw", "straw2"
	Items     []string // 이 버킷에 포함된 하위 아이템(OSD 또는 다른 버킷 이름)
}
