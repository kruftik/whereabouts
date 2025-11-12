// Package main contains the beginning of the wereabouts cmd
package main

import (
	"context"
	"fmt"
	"net"

	"github.com/containernetworking/cni/pkg/skel"
	cnitypes "github.com/containernetworking/cni/pkg/types"
	current "github.com/containernetworking/cni/pkg/types/100"
	cniversion "github.com/containernetworking/cni/pkg/version"

	"github.com/k8snetworkplumbingwg/whereabouts/pkg/config"
	"github.com/k8snetworkplumbingwg/whereabouts/pkg/logging"
	"github.com/k8snetworkplumbingwg/whereabouts/pkg/storage/kubernetes"
	"github.com/k8snetworkplumbingwg/whereabouts/pkg/types"
	"github.com/k8snetworkplumbingwg/whereabouts/pkg/version"
)

type IPAM interface {
	Allocate(
		ctx context.Context,
		ipamConf types.IPAMConfig,
		poolNS, podRef, containerID, ifName string,
	) ([]net.IPNet, error)

	Deallocate(
		ctx context.Context,
		ipamConf types.IPAMConfig,
		poolNS, podRef, containerID, ifName string,
	) error
}

func cmdAddFunc(args *skel.CmdArgs) error {
	ipamConf, confVersion, err := config.LoadIPAMConfig(args.StdinData, args.Args)
	if err != nil {
		logging.Errorf("IPAMService configuration load failed: %s", err)
		return err
	}
	logging.Debugf("ADD - IPAMService configuration successfully read: %+v", *ipamConf)
	ipam, err := kubernetes.NewKubernetesIPAM(args.ContainerID, args.IfName, *ipamConf)
	if err != nil {
		return logging.Errorf("failed to create Kubernetes IPAMService manager: %v", err)
	}
	defer func() { safeCloseKubernetesBackendConnection(ipam) }()

	ipamSvc := kubernetes.NewIPAMService(&ipam.Client)

	logging.Debugf("Beginning IPAMService for ContainerID: %q - podRef: %q - ifName: %q", args.ContainerID, ipamConf.GetPodRef(), args.IfName)
	return cmdAdd(
		confVersion,
		ipamSvc,
		ipam.Config,
		ipam.Namespace,
		ipam.Config.GetPodRef(),
		ipam.ContainerID,
		ipam.IfName,
	)
}

func cmdDelFunc(args *skel.CmdArgs) error {
	ipamConf, _, err := config.LoadIPAMConfig(args.StdinData, args.Args)
	if err != nil {
		logging.Errorf("IPAMService configuration load failed: %s", err)
		return err
	}
	logging.Debugf("DEL - IPAMService configuration successfully read: %+v", *ipamConf)

	ipam, err := kubernetes.NewKubernetesIPAM(args.ContainerID, args.IfName, *ipamConf)
	if err != nil {
		return logging.Errorf("IPAMService client initialization error: %v", err)
	}
	defer func() { safeCloseKubernetesBackendConnection(ipam) }()

	ipamSvc := kubernetes.NewIPAMService(&ipam.Client)

	logging.Debugf("Beginning delete for ContainerID: %q - podRef: %q - ifName: %q", args.ContainerID, ipamConf.GetPodRef(), args.IfName)
	return cmdDel(
		ipamSvc,
		ipam.Config,
		ipam.Namespace,
		ipam.Config.GetPodRef(),
		ipam.ContainerID,
		ipam.IfName,
	)
}

func main() {
	skel.PluginMainFuncs(skel.CNIFuncs{
		Add:   cmdAddFunc,
		Check: cmdCheck,
		Del:   cmdDelFunc,
	},
		cniversion.All,
		fmt.Sprintf("whereabouts %s", version.GetFullVersionWithRuntimeInfo()))
}

func safeCloseKubernetesBackendConnection(ipam *kubernetes.KubernetesIPAM) {
	if err := ipam.Close(); err != nil {
		_ = logging.Errorf("failed to close the connection to the K8s backend: %v", err)
	}
}

func cmdCheck(args *skel.CmdArgs) error {
	// TODO
	return fmt.Errorf("CNI CHECK method is not implemented")
}

func cmdAdd(
	cniVersion string,
	ipam IPAM,
	ipamConf types.IPAMConfig,
	poolNS, podRef, containerID, ifName string,
) error {
	// Initialize our result, and assign DNS & routing.
	result := &current.Result{}
	result.DNS = ipamConf.DNS
	result.Routes = ipamConf.Routes

	var newips []net.IPNet

	ctx, cancel := context.WithTimeout(context.Background(), types.AddTimeLimit)
	defer cancel()

	newips, err := ipam.Allocate(ctx, ipamConf, poolNS, podRef, containerID, ifName)
	if err != nil {
		logging.Errorf("Error at storage engine: %s", err)
		return fmt.Errorf("error at storage engine: %w", err)
	}

	for _, newip := range newips {
		result.IPs = append(result.IPs, &current.IPConfig{
			Address: newip,
			Gateway: ipamConf.Gateway})
	}

	// Assign all the static IP elements.
	for _, v := range ipamConf.Addresses {
		result.IPs = append(result.IPs, &current.IPConfig{
			Address: v.Address,
			Gateway: v.Gateway})
	}

	return cnitypes.PrintResult(result, cniVersion)
}

func cmdDel(
	ipam IPAM,
	ipamConf types.IPAMConfig,
	poolNS, podRef, containerID, ifName string,
) error {
	ctx, cancel := context.WithTimeout(context.Background(), types.DelTimeLimit)
	defer cancel()

	return ipam.Deallocate(ctx, ipamConf, poolNS, podRef, containerID, ifName)
}
