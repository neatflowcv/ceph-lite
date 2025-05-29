package crush

import "fmt"

// Device는 OSD(Object Storage Device)를 나타냅니다.
type Device struct {
	OSDID  int
	Weight float64
	// 실제 Ceph에서는 더 많은 속성(예: class)이 있을 수 있습니다.
}

func NewDevice(osdID int, weight float64) *Device {
	return &Device{
		OSDID:  osdID,
		Weight: weight,
	}
}

func (d *Device) Key() string {
	return fmt.Sprintf("osd.%d", d.OSDID)
}

func (d *Device) GetWeight() float64 {
	return d.Weight
}
