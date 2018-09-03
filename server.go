// Copyright (c) 2017, NVIDIA CORPORATION. All rights reserved.

package main

import (
	"fmt"
	"log"
	"net"
	"os"
	"path"
	"time"

	"golang.org/x/net/context"
	"google.golang.org/grpc"
	pluginapi "k8s.io/kubernetes/pkg/kubelet/apis/deviceplugin/v1beta1"
	"sort"
	"strings"
)

const (
	resourceName           = "nvidia.com/gpu"
	serverSock             = pluginapi.DevicePluginPath + "nvidia.sock"
	envDisableHealthChecks = "DP_DISABLE_HEALTHCHECKS"
	allHealthChecks        = "xids"
)

// NvidiaDevicePlugin implements the Kubernetes device plugin API
type NvidiaDevicePlugin struct {
	devs     []*pluginapi.Device
	topology TopoInfo
	socket   string

	stop   chan interface{}
	health chan *pluginapi.Device

	server *grpc.Server
}

// Test: A stub, just provides a virtual gpu status
func testGetDevices() []*pluginapi.Device {
	n := uint(8)

	var devs []*pluginapi.Device
	for i := uint(0); i < n; i++ {
		switch i {
		case 0:
			devs = append(devs, &pluginapi.Device{
				ID:     "GPU-92d93cd6-e41f-6884-6748-3738a97691df",
				Health: pluginapi.Healthy,
			})
			break
		case 1:
			devs = append(devs, &pluginapi.Device{
				ID:     "GPU-7ea160c1-73de-f6f4-1d3d-e34340d85eef",
				Health: pluginapi.Healthy,
			})
			break
		case 2:
			devs = append(devs, &pluginapi.Device{
				ID:     "GPU-92d93cd6-e41f-6884-6748-3738a976fff1",
				Health: pluginapi.Healthy,
			})
			break
		case 3:
			devs = append(devs, &pluginapi.Device{
				ID:     "GPU-92d93cd6-e41f-6884-6748-3738a976fff2",
				Health: pluginapi.Healthy,
			})
			break
		case 4:
			devs = append(devs, &pluginapi.Device{
				ID:     "GPU-92d93cd6-e41f-6884-6748-3738a976fff3",
				Health: pluginapi.Healthy,
			})
			break
		case 5:
			devs = append(devs, &pluginapi.Device{
				ID:     "GPU-92d93cd6-e41f-6884-6748-3738a976fff4",
				Health: pluginapi.Healthy,
			})
			break
		case 6:
			devs = append(devs, &pluginapi.Device{
				ID:     "GPU-92d93cd6-e41f-6884-6748-3738a976fff5",
				Health: pluginapi.Healthy,
			})
			break
		case 7:
			devs = append(devs, &pluginapi.Device{
				ID:     "GPU-92d93cd6-e41f-6884-6748-3738a976fff6",
				Health: pluginapi.Healthy,
			})
			break
		}
	}

	return devs
}

// NewNvidiaDevicePlugin returns an initialized NvidiaDevicePlugin
func NewNvidiaDevicePlugin() *NvidiaDevicePlugin {
	devicesIDsAndHealth := testGetDevices()
	topo := TopoInfo{nil, nil}
	//devicesIDsAndHealth, topo := getDevicesAndTopology()
	log.Println("Test topology: ", topo)
	return &NvidiaDevicePlugin{
		devs:     devicesIDsAndHealth,
		topology: topo,
		socket:   serverSock,

		stop:   make(chan interface{}),
		health: make(chan *pluginapi.Device),
	}
}

func (m *NvidiaDevicePlugin) GetDevicePluginOptions(context.Context, *pluginapi.Empty) (*pluginapi.DevicePluginOptions, error) {
	return &pluginapi.DevicePluginOptions{
		PreStartRequired:    false,
		PreAllocateRequired: false,
	}, nil
}

// dial establishes the gRPC communication with the registered device plugin.
func dial(unixSocketPath string, timeout time.Duration) (*grpc.ClientConn, error) {
	c, err := grpc.Dial(unixSocketPath, grpc.WithInsecure(), grpc.WithBlock(),
		grpc.WithTimeout(timeout),
		grpc.WithDialer(func(addr string, timeout time.Duration) (net.Conn, error) {
			return net.DialTimeout("unix", addr, timeout)
		}),
	)

	if err != nil {
		return nil, err
	}

	return c, nil
}

// Start starts the gRPC server of the device plugin
func (m *NvidiaDevicePlugin) Start() error {
	err := m.cleanup()
	if err != nil {
		return err
	}

	sock, err := net.Listen("unix", m.socket)
	if err != nil {
		return err
	}

	m.server = grpc.NewServer([]grpc.ServerOption{}...)
	pluginapi.RegisterDevicePluginServer(m.server, m)

	go m.server.Serve(sock)

	// Wait for server to start by launching a blocking connexion
	conn, err := dial(m.socket, 5*time.Second)
	if err != nil {
		return err
	}
	conn.Close()

	go m.healthcheck()

	return nil
}

// Stop stops the gRPC server
func (m *NvidiaDevicePlugin) Stop() error {
	if m.server == nil {
		return nil
	}

	m.server.Stop()
	m.server = nil
	close(m.stop)

	return m.cleanup()
}

// Register registers the device plugin for the given resourceName with Kubelet.
func (m *NvidiaDevicePlugin) Register(kubeletEndpoint, resourceName string) error {
	conn, err := dial(kubeletEndpoint, 5*time.Second)
	if err != nil {
		return err
	}
	defer conn.Close()

	client := pluginapi.NewRegistrationClient(conn)
	reqt := &pluginapi.RegisterRequest{
		Version:      pluginapi.Version,
		Endpoint:     path.Base(m.socket),
		ResourceName: resourceName,
		Options: &pluginapi.DevicePluginOptions{
			PreStartRequired:    false,
			PreAllocateRequired: false,
		},
	}

	_, err = client.Register(context.Background(), reqt)
	if err != nil {
		return err
	}
	return nil
}

// ListAndWatch lists devices and update that list according to the health status
func (m *NvidiaDevicePlugin) ListAndWatch(e *pluginapi.Empty, s pluginapi.DevicePlugin_ListAndWatchServer) error {
	s.Send(&pluginapi.ListAndWatchResponse{Devices: m.devs})

	for {
		select {
		case <-m.stop:
			return nil
		case d := <-m.health:
			// FIXME: there is no way to recover from the Unhealthy state.
			d.Health = pluginapi.Unhealthy
			s.Send(&pluginapi.ListAndWatchResponse{Devices: m.devs})
		}
	}
}

func (m *NvidiaDevicePlugin) unhealthy(dev *pluginapi.Device) {
	m.health <- dev
}

// Allocate which return list of devices.
func (m *NvidiaDevicePlugin) Allocate(ctx context.Context, reqs *pluginapi.AllocateRequest) (*pluginapi.AllocateResponse, error) {
	devs := m.devs
	responses := pluginapi.AllocateResponse{}
	for _, req := range reqs.ContainerRequests {
		response := pluginapi.ContainerAllocateResponse{
			Envs: map[string]string{
				"NVIDIA_VISIBLE_DEVICES": strings.Join(req.DevicesIDs, ","),
			},
		}

		for _, id := range req.DevicesIDs {
			if !deviceExists(devs, id) {
				return nil, fmt.Errorf("invalid allocation request: unknown device: %s", id)
			}
		}

		responses.ContainerResponses = append(responses.ContainerResponses, &response)
	}

	return &responses, nil
}

func (m *NvidiaDevicePlugin) PreStartContainer(context.Context, *pluginapi.PreStartContainerRequest) (*pluginapi.PreStartContainerResponse, error) {
	return &pluginapi.PreStartContainerResponse{}, nil
}

func (m *NvidiaDevicePlugin) PreAllocate(ctx context.Context, request *pluginapi.PreAllocateRequest) (*pluginapi.PreAllocateResponse, error) {
	return m.scheduleTestStub(request)
}

func (m *NvidiaDevicePlugin) cleanup() error {
	if err := os.Remove(m.socket); err != nil && !os.IsNotExist(err) {
		return err
	}

	return nil
}

func (m *NvidiaDevicePlugin) healthcheck() {
	/*disableHealthChecks := strings.ToLower(os.Getenv(envDisableHealthChecks))
	if disableHealthChecks == "all" {
		disableHealthChecks = allHealthChecks
	}

	ctx, cancel := context.WithCancel(context.Background())

	var xids chan *pluginapi.Device
	if !strings.Contains(disableHealthChecks, "xids") {
		xids = make(chan *pluginapi.Device)
		go watchXIDs(ctx, m.devs, xids)
	}

	for {
		select {
		case <-m.stop:
			cancel()
			return
		case dev := <-xids:
			m.unhealthy(dev)
		}
	}*/
}

// Serve starts the gRPC server and register the device plugin to Kubelet
func (m *NvidiaDevicePlugin) Serve() error {
	err := m.Start()
	if err != nil {
		log.Printf("Could not start device plugin: %s", err)
		return err
	}
	log.Println("Starting to serve on", m.socket)

	err = m.Register(pluginapi.KubeletSocket, resourceName)
	if err != nil {
		log.Printf("Could not register device plugin: %s", err)
		m.Stop()
		return err
	}
	log.Println("Registered device plugin with Kubelet")

	return nil
}

func (m *NvidiaDevicePlugin) scheduleByTopoEdge(request *pluginapi.PreAllocateRequest) (*pluginapi.PreAllocateResponse, error) {
	var selectedDevicesIDs []string
	// For each edge test weather the devices it connect are usable
	var usableTopoInfo TopoInfo
	for _, edge := range m.topology.edges {
		leftPointExist := false
		rightPointExist := false
		for _, usableDevID := range request.UsableDevicesIDs {
			if usableDevID == edge.Dev1UUID {
				leftPointExist = true
			}
			if usableDevID == edge.Dev2UUID {
				rightPointExist = true
			}
		}
		if leftPointExist && rightPointExist {
			usableTopoInfo.edges = append(usableTopoInfo.edges, edge)
		}
	}
	// Sort edges
	usableTopoInfo.TopoEdgeSort()
	// Select devices
	for _, edge := range usableTopoInfo.edges {
		leftExisted := false
		rightExisted := false
		for _, id := range selectedDevicesIDs {
			if id == edge.Dev1UUID {
				leftExisted = true
			}
			if id == edge.Dev2UUID {
				rightExisted = true
			}
		}
		if len(selectedDevicesIDs) >= int(request.DevicesNum) {
			break
		}
		if !leftExisted {
			selectedDevicesIDs = append(selectedDevicesIDs, edge.Dev1UUID)
		}
		if len(selectedDevicesIDs) >= int(request.DevicesNum) {
			break
		}
		if !rightExisted {
			selectedDevicesIDs = append(selectedDevicesIDs, edge.Dev2UUID)
		}
	}

	return &pluginapi.PreAllocateResponse{
		SelectedDevicesIDs: selectedDevicesIDs,
	}, nil
}

func (m *NvidiaDevicePlugin) scheduleByGraphSearching(request *pluginapi.PreAllocateRequest) (*pluginapi.PreAllocateResponse, error) {
	var selectedDevicesIDs []string
	// Perpare a dictionary to accelerate searching weather a ID exists.
	usableIDSet := make(map[string]Empty)
	for _, usableID := range request.UsableDevicesIDs {
		usableIDSet[usableID] = Empty{}
	}
	// Select out the usable devices.
	usableDevices := make(ConnectedNodeList, request.DevicesNum)
	i := 0
	for k, v := range m.topology.connectGraph {
		if _, ok := usableIDSet[k]; ok {
			if i < int(request.DevicesNum) {
				usableDevices[i] = ConnectedNodePacked{k, v.score}
				i++
			} else {
				break
			}
		}
	}
	// Sort devices from high to low
	sort.Sort(usableDevices)
	// Select
	for j := 0; j < int(request.DevicesNum); j++ {
		selectedDevicesIDs = append(selectedDevicesIDs, usableDevices[j].UUID)
	}

	return &pluginapi.PreAllocateResponse{
		SelectedDevicesIDs: selectedDevicesIDs,
	}, nil
}

func (m *NvidiaDevicePlugin) scheduleTestStub(request *pluginapi.PreAllocateRequest) (*pluginapi.PreAllocateResponse, error) {
	var selectedDevicesIDs []string

	// Select
	for j := 0; j < int(request.DevicesNum); j++ {
		selectedDevicesIDs = append(selectedDevicesIDs, request.UsableDevicesIDs[j])
	}

	return &pluginapi.PreAllocateResponse{
		SelectedDevicesIDs: selectedDevicesIDs,
	}, nil
}
