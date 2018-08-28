// Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.

package main

import (
	"log"
	"strings"

	"github.com/NVIDIA/gpu-monitoring-tools/bindings/go/nvml"

	"fmt"
	"golang.org/x/net/context"
	pluginapi "k8s.io/kubernetes/pkg/kubelet/apis/deviceplugin/v1beta1"
)

type TopoInfo struct {
	edges []TopoEdge
}

type TopoEdge struct {
	Dev1UUID string
	Dev2UUID string
	P2PType  nvml.P2PLinkType
}

func check(err error) {
	if err != nil {
		log.Panicln("Fatal:", err)
	}
}

func (topo *TopoInfo) TopoEdgeSort() {
	n := len(topo.edges)
	if n <= 0 {
		return
	}
	topo.topoEdgeQuickSort(0, n-1)
}

func (topo *TopoInfo) topoEdgeQuickSort(start int, end int) {
	// Quick Sort
	lo := start
	hi := end
	mi := lo
	reverse := true
	for hi != lo {
		if reverse {
			if topo.topoEdgeCompare_First_QuickOrEqual_Second(hi, mi) {
				hi--
				if hi == lo {
					if mi-1 > start {
						topo.topoEdgeQuickSort(start, mi-1)
					}
					if mi+1 < end {
						topo.topoEdgeQuickSort(mi+1, end)
					}
				}
			} else {
				temp := topo.edges[mi]
				topo.edges[mi] = topo.edges[hi]
				topo.edges[hi] = temp
				mi = hi
				reverse = false
			}
		} else {
			if topo.topoEdgeCompare_First_QuickOrEqual_Second(mi, lo) {
				lo++
				if hi == lo {
					if mi-1 > start {
						topo.topoEdgeQuickSort(start, mi-1)
					}
					if mi+1 < end {
						topo.topoEdgeQuickSort(mi+1, end)
					}
				}
			} else {
				temp := topo.edges[mi]
				topo.edges[mi] = topo.edges[lo]
				topo.edges[lo] = temp
				mi = lo
				reverse = true
			}
		}
	}
	return
}

func (topo *TopoInfo) topoEdgeCompare_First_QuickOrEqual_Second(e1 int, e2 int) bool {
	return topo.edges[e1].P2PType >= topo.edges[e2].P2PType
}

func getDevicesCount() uint {
	n, err := nvml.GetDeviceCount()
	check(err)

	return n
}

func getDevices() []*pluginapi.Device {
	n, err := nvml.GetDeviceCount()
	check(err)

	var devs []*pluginapi.Device
	for i := uint(0); i < n; i++ {
		d, err := nvml.NewDeviceLite(i)
		check(err)
		devs = append(devs, &pluginapi.Device{
			ID:     d.UUID,
			Health: pluginapi.Healthy,
		})
	}

	return devs
}

func getDevicesAndTopology() ([]*pluginapi.Device, TopoInfo) {
	n, err := nvml.GetDeviceCount()
	check(err)

	var devs []*pluginapi.Device
	var nvmlDevs []*nvml.Device
	for i := uint(0); i < n; i++ {
		d, err := nvml.NewDeviceLite(i)
		check(err)
		nvmlDevs = append(nvmlDevs, d)
		devs = append(devs, &pluginapi.Device{
			ID:     d.UUID,
			Health: pluginapi.Healthy,
		})
	}

	var topo TopoInfo
	for i := uint(0); i < n; i++ {
		for j := i + 1; j < n; j++ {
			p2pType, err := nvml.GetP2PLink(nvmlDevs[i], nvmlDevs[j])
			if err != nil {
				fmt.Errorf("GetP2PLink Error: %v", err)
				return devs, TopoInfo{}
			}
			topo.edges = append(topo.edges, TopoEdge{
				Dev1UUID: nvmlDevs[i].UUID,
				Dev2UUID: nvmlDevs[j].UUID,
				P2PType:  p2pType,
			})
		}
	}

	return devs, topo
}

func deviceExists(devs []*pluginapi.Device, id string) bool {
	for _, d := range devs {
		if d.ID == id {
			return true
		}
	}
	return false
}

func watchXIDs(ctx context.Context, devs []*pluginapi.Device, xids chan<- *pluginapi.Device) {
	eventSet := nvml.NewEventSet()
	defer nvml.DeleteEventSet(eventSet)

	for _, d := range devs {
		err := nvml.RegisterEventForDevice(eventSet, nvml.XidCriticalError, d.ID)
		if err != nil && strings.HasSuffix(err.Error(), "Not Supported") {
			log.Printf("Warning: %s is too old to support healthchecking: %s. Marking it unhealthy.", d.ID, err)

			xids <- d
			continue
		}

		if err != nil {
			log.Panicln("Fatal:", err)
		}
	}

	for {
		select {
		case <-ctx.Done():
			return
		default:
		}

		e, err := nvml.WaitForEvent(eventSet, 5000)
		if err != nil && e.Etype != nvml.XidCriticalError {
			continue
		}

		// FIXME: formalize the full list and document it.
		// http://docs.nvidia.com/deploy/xid-errors/index.html#topic_4
		// Application errors: the GPU should still be healthy
		if e.Edata == 31 || e.Edata == 43 || e.Edata == 45 {
			continue
		}

		if e.UUID == nil || len(*e.UUID) == 0 {
			// All devices are unhealthy
			for _, d := range devs {
				xids <- d
			}
			continue
		}

		for _, d := range devs {
			if d.ID == *e.UUID {
				xids <- d
			}
		}
	}
}
