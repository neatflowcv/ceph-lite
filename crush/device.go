package crush

import "fmt"

// Device는 OSD(Object Storage Device)를 나타냅니다.
type Device struct {
	osdID  int
	weight float64
	// 실제 Ceph에서는 더 많은 속성(예: class)이 있을 수 있습니다.
}

func NewDevice(osdID int, weight float64) *Device {
	return &Device{
		osdID:  osdID,
		weight: weight,
	}
}

func (d *Device) Key() string {
	return fmt.Sprintf("osd.%d", d.osdID)
}

func (d *Device) Weight() float64 {
	return d.weight
}
