package main

type Empty struct{}

type UsableDeviceMap map[string]bool

type ConnectGraph map[string]ConnectedNode

type LinkSpeed int

type ConnectedNode struct {
	Score           int
	ConnectedDevice map[string]LinkSpeed
}

type ConnectedNodePacked struct {
	UUID  string
	score int
}

type ConnectedNodeList []ConnectedNodePacked

// Sort interface for ConnectedNodeList
func (c ConnectedNodeList) Swap(i, j int) {
	c[i], c[j] = c[j], c[i]
}

func (c ConnectedNodeList) Len() int {
	return len(c)
}

func (c ConnectedNodeList) Less(i, j int) bool {
	return c[i].score < c[j].score
}
