package crush_test

import (
	"testing"

	"github.com/neatflowcv/ceph-lite/crush"
	"github.com/stretchr/testify/require"
)

func TestCrush(t *testing.T) {
	myCrushMap := crush.NewCrushMap()

	// OSD 추가 (가중치는 디스크 용량 등에 비례)
	myCrushMap.AddDevice(0, 1.0) // osd.0
	myCrushMap.AddDevice(1, 1.0) // osd.1
	myCrushMap.AddDevice(2, 1.0) // osd.2
	myCrushMap.AddDevice(3, 1.0) // osd.3

	// 호스트 버킷 정의
	myCrushMap.AddBucket("host_node1", "host", "straw", []string{"osd.0", "osd.1"})
	myCrushMap.AddBucket("host_node2", "host", "straw", []string{"osd.2", "osd.3"})

	// 랙 버킷 정의 (각 랙에 호스트 포함)
	myCrushMap.AddBucket("rack_a", "rack", "straw", []string{"host_node1"})
	myCrushMap.AddBucket("rack_b", "rack", "straw", []string{"host_node2"})

	// 루트 버킷 정의 (모든 랙 포함) - 'default' root 버킷
	myCrushMap.AddBucket("default", "root", "straw", []string{"rack_a", "rack_b"})
	myCrushMap.RootID = myCrushMap.Buckets["default"].ID

	// 복제 규칙 정의 (3개의 복제본을 서로 다른 랙에 저장)
	replicationRuleSteps := []crush.RuleStep{
		{Op: "take", Item: "default"},                          // default root에서 시작
		{Op: "chooseleaf", Num: 3, Type: "rack", Class: "hdd"}, // 서로 다른 랙에서 3개의 OSD 선택 (hdd 클래스 가정)
		{Op: "emit"}, // 결과 반환
	}
	myCrushMap.AddRule("replicated_rule", replicationRuleSteps)

	// PG ID에 따른 OSD 선택 시뮬레이션
	pgIDToFind := 100
	chosenOSDs, err := crush.CrushDoRule(pgIDToFind, myCrushMap, "replicated_rule")
	require.NoError(t, err)
	require.Equal(t, []string{"osd.0", "osd.2"}, chosenOSDs)

	pgIDToFind2 := 101
	chosenOSDs2, err := crush.CrushDoRule(pgIDToFind2, myCrushMap, "replicated_rule")
	require.NoError(t, err)
	require.Equal(t, []string{"osd.0", "osd.3"}, chosenOSDs2)
}
