package crush

// RuleStep은 CRUSH 규칙 내의 단일 작업을 나타냅니다.
type RuleStep struct {
	Op    string // 예: "take", "chooseleaf", "emit"
	Item  string // 'take' 오퍼레이션의 시작 아이템
	Num   int    // 'chooseleaf' 오퍼레이션에서 선택할 아이템 수
	Type  string // 'chooseleaf' 오퍼레이션에서 고려할 실패 도메인 타입 (예: "rack", "host")
	Class string // 'take' 오퍼레이션에서 필터링할 OSD 클래스 (예: "hdd")
}
