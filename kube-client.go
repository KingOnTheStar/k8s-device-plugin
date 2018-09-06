package main

import (
	"encoding/json"
	"flag"
	metav1 "k8s.io/apimachinery/pkg/apis/meta/v1"
	"k8s.io/client-go/kubernetes"
	restclient "k8s.io/client-go/rest"
	"k8s.io/client-go/tools/clientcmd"
	"log"
	"os"
)

type KubeClient struct {
	Clientset *kubernetes.Clientset
	Config    *restclient.Config
}

func NewKubeClient(kubeconfigPath string) *KubeClient {
	var kubeconfig *string
	kubeconfig = flag.String("kubeconfig", kubeconfigPath, "absolute path to the kubeconfig file")
	flag.Parse()

	// use the current context in kubeconfig
	config, err := clientcmd.BuildConfigFromFlags("", *kubeconfig)
	if err != nil {
		panic(err.Error())
	}

	// create the clientset
	clientset, err := kubernetes.NewForConfig(config)
	if err != nil {
		panic(err.Error())
	}

	return &KubeClient{
		Clientset: clientset,
		Config:    config,
	}
}

func (c *KubeClient) TestNodeAnnotation() {
	node, err := c.Clientset.CoreV1().Nodes().List(metav1.ListOptions{})
	if err != nil {
		panic(err.Error())
	}

	node.Items[0].Annotations["Topology"] = "HelloWorld"

	c.Clientset.CoreV1().Nodes().Update(&node.Items[0])
}

func (c *KubeClient) SetTopologyAnnotation(devicePlugin *NvidiaDevicePlugin) {
	data, err := json.Marshal(devicePlugin.topology.edges)
	if err != nil {
		log.Println("SetTopologyAnnotation:: Failed to create json.")
	}

	nodeName := os.Getenv("HOST_IP")

	node, err := c.Clientset.CoreV1().Nodes().Get(nodeName, metav1.GetOptions{})
	if err != nil {
		panic(err.Error())
	}

	node.Annotations["Topology"] = string(data)

	c.Clientset.CoreV1().Nodes().Update(node)
}
