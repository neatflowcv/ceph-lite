package crush

// Device는 OSD(Object Storage Device)를 나타냅니다.
type Device struct {
	Weight float64
	// 실제 Ceph에서는 더 많은 속성(예: class)이 있을 수 있습니다.
}
