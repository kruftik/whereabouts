package kubernetes

import (
	"context"
	"errors"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/k8snetworkplumbingwg/whereabouts/pkg/allocate"
	"github.com/k8snetworkplumbingwg/whereabouts/pkg/iphelpers"
	"github.com/k8snetworkplumbingwg/whereabouts/pkg/logging"
	"github.com/k8snetworkplumbingwg/whereabouts/pkg/storage"
	whereaboutstypes "github.com/k8snetworkplumbingwg/whereabouts/pkg/types"
)

type RetriableError struct {
	error
}

type IPAMService struct {
	client *Client
}

func NewIPAMService(client *Client) IPAMService {
	return IPAMService{
		client: client,
	}
}

func (s IPAMService) Allocate(
	ctx context.Context,
	ipamConf whereaboutstypes.IPAMConfig,
	poolNS, podRef, containerID, ifName string,
) ([]net.IPNet, error) {
	logging.Debugf("IPAMService Allocate")

	return s.executeOperation(ctx, ipamConf, whereaboutstypes.Allocate, poolNS, podRef, containerID, ifName)
}

func (s IPAMService) Deallocate(
	ctx context.Context,
	ipamConf whereaboutstypes.IPAMConfig,
	poolNS, podRef, containerID, ifName string,
) error {
	logging.Debugf("IPAMService Deallocate")

	_, err := s.executeOperation(ctx, ipamConf, whereaboutstypes.Deallocate, poolNS, podRef, containerID, ifName)

	return err
}

func (s IPAMService) retriableAllocateDeallocateFromRange(
	ctx context.Context,
	ipam *KubernetesIPAM,
	ipamConf whereaboutstypes.IPAMConfig,
	overlappingrangestore storage.OverlappingRangeStore,
	overlappingrangeallocations []whereaboutstypes.IPReservation,
	ipRange whereaboutstypes.RangeConfiguration,
	op whereaboutstypes.OperationType,
	podRef, containerID, ifName string,
) (net.IPNet, []whereaboutstypes.IPReservation, bool, error) {
	var (
		newip net.IPNet

		skipOverlappingRangeUpdate bool

		err error
	)

	ctx, cancelFn := context.WithTimeout(ctx, storage.RequestTimeout)
	defer cancelFn()

	poolIdentifier := PoolIdentifier{IpRange: ipRange.Range, NetworkName: ipamConf.NetworkName}
	if ipamConf.NodeSliceSize != "" {
		ipRange, poolIdentifier, err = s.getNodeSlicedRangeAndPoolConfiguration(
			ctx,
			ipam,
			ipRange,
			poolIdentifier,
		)
		if err != nil {
			return net.IPNet{}, overlappingrangeallocations, skipOverlappingRangeUpdate, fmt.Errorf("cannot get node-sliced configuration: %w", err)
		}
	}

	logging.Debugf("using pool identifier: %v", poolIdentifier)

	pool, err := ipam.GetIPPool(ctx, poolIdentifier)
	if err != nil {
		err := fmt.Errorf("cannot read IPAM pool allocation: %w", err)
		var tmpErr storage.Temporary
		if ok := errors.As(err, &tmpErr); ok && tmpErr.Temporary() {
			return net.IPNet{}, overlappingrangeallocations, skipOverlappingRangeUpdate, RetriableError{err}
		}
		return net.IPNet{}, overlappingrangeallocations, skipOverlappingRangeUpdate, err
	}

	var (
		reservelist        = append(pool.Allocations(), overlappingrangeallocations...)
		updatedreservelist []whereaboutstypes.IPReservation
	)

	switch op {
	case whereaboutstypes.Allocate:
		newip, updatedreservelist, err = allocate.AssignIP(ipRange, reservelist, containerID, podRef, ifName)
		if err != nil {
			return net.IPNet{}, overlappingrangeallocations, skipOverlappingRangeUpdate, fmt.Errorf("cannot assign IP: %w", err)
		}

		// Now check if this is allocated overlappingrange wide
		// When it's allocated overlappingrange wide, we add it to a local reserved list
		// And we try again.
		if ipamConf.OverlappingRanges {
			overlappingRangeIPReservation, err := overlappingrangestore.GetOverlappingRangeIPReservation(ctx, newip.IP,
				podRef, ipamConf.NetworkName)
			if err != nil {
				return newip, overlappingrangeallocations, skipOverlappingRangeUpdate, fmt.Errorf("cannot get cluster wide IP allocation: %w", err)
			}

			if overlappingRangeIPReservation != nil {
				if overlappingRangeIPReservation.Spec.PodRef != podRef {
					logging.Debugf("Continuing loop, IP is already allocated (possibly from another range): %v", newip)
					// We create "dummy" records here for evaluation, but, we need to filter those out later.
					overlappingrangeallocations = append(overlappingrangeallocations, whereaboutstypes.IPReservation{IP: newip.IP, IsAllocated: true})
					return newip, overlappingrangeallocations, skipOverlappingRangeUpdate, RetriableError{fmt.Errorf("IP is already allocated (possibly from another range): %v", newip)}
				}

				skipOverlappingRangeUpdate = true
			}
		}

	case whereaboutstypes.Deallocate:
		var ipforoverlappingrangeupdate net.IP
		updatedreservelist, ipforoverlappingrangeupdate = allocate.DeallocateIP(reservelist, containerID, ifName)
		if ipforoverlappingrangeupdate == nil {
			// Do not fail if allocation was not found.
			logging.Debugf("Failed to find allocation for container ID: %s", containerID)
			return net.IPNet{}, overlappingrangeallocations, skipOverlappingRangeUpdate, nil
		}
		newip = net.IPNet{
			IP: ipforoverlappingrangeupdate,
		}
	}

	// Clean out any dummy records from the reservelist...
	var usereservelist []whereaboutstypes.IPReservation
	for _, rl := range updatedreservelist {
		if !rl.IsAllocated {
			usereservelist = append(usereservelist, rl)
		}
	}

	// Manual race condition testing
	if ipamConf.SleepForRace > 0 {
		time.Sleep(time.Duration(ipamConf.SleepForRace) * time.Second)
	}

	err = pool.Update(ctx, usereservelist)
	if err != nil {
		err = fmt.Errorf("cannot update IPAM pool allocations: %w", err)
		var tmpErr storage.Temporary
		if ok := errors.As(err, &tmpErr); ok && tmpErr.Temporary() {
			return net.IPNet{}, overlappingrangeallocations, skipOverlappingRangeUpdate, RetriableError{err}
		}
		return net.IPNet{}, overlappingrangeallocations, skipOverlappingRangeUpdate, err
	}

	return newip, overlappingrangeallocations, skipOverlappingRangeUpdate, nil
}

func (s IPAMService) allocateDeallocateFromRange(
	ctx context.Context,
	ipam *KubernetesIPAM,
	ipamConf whereaboutstypes.IPAMConfig,
	overlappingrangestore storage.OverlappingRangeStore,
	ipRange whereaboutstypes.RangeConfiguration,
	op whereaboutstypes.OperationType,
	podRef, containerID, ifName string,
) (net.IPNet, error) {
	var (
		newip net.IPNet

		// handle the ip add/del until successful
		overlappingrangeallocations []whereaboutstypes.IPReservation

		skipOverlappingRangeUpdate bool

		err error
	)

	for j := 0; j < storage.DatastoreRetries; j++ {
		select {
		case <-ctx.Done():
			return net.IPNet{}, ctx.Err()
		default:
			// retry the IPAMService loop if the context has not been cancelled
		}

		newip, overlappingrangeallocations, skipOverlappingRangeUpdate, err = s.retriableAllocateDeallocateFromRange(
			ctx,
			ipam,
			ipamConf,
			overlappingrangestore,
			overlappingrangeallocations,
			ipRange,
			op,
			podRef,
			containerID,
			ifName,
		)
		if err != nil {
			if errors.As(err, new(RetriableError)) {
				logging.Debugf("cannot allocate IP (attempt: %d): %v", j, err)
				continue
			}
			return net.IPNet{}, fmt.Errorf("cannot allocate IP: %w", err)
		}
		break
	}

	if err != nil {
		return net.IPNet{}, fmt.Errorf("cannot allocate IP: %w", err)
	}

	requestCtx, cancelFn := context.WithTimeout(ctx, storage.RequestTimeout)
	defer cancelFn()

	if ipamConf.OverlappingRanges && !skipOverlappingRangeUpdate {
		err := overlappingrangestore.UpdateOverlappingRangeAllocation(requestCtx, op, newip.IP,
			podRef, ifName, ipamConf.NetworkName)
		if err != nil {
			return net.IPNet{}, fmt.Errorf("cannot update overlapping range allocation: %w", err)
		}
	}

	return newip, nil
}

func (s IPAMService) runOperation(
	ctx context.Context,
	ipam *KubernetesIPAM,
	ipamConf whereaboutstypes.IPAMConfig,
	op whereaboutstypes.OperationType,
	poolNS, podRef, containerID, ifName string,
) ([]net.IPNet, error) {
	logging.Debugf("IPManagement -- mode: %d / containerID: %q / podRef: %q / ifName: %q ", op, containerID, podRef, ifName)

	// Skip invalid modes
	switch op {
	case whereaboutstypes.Allocate, whereaboutstypes.Deallocate:
	default:
		return nil, fmt.Errorf("got an unknown mode passed to IPManagement: %v", op)
	}

	requestCtx, requestCancel := context.WithTimeout(ctx, storage.RequestTimeout)
	defer requestCancel()

	// Check our connectivity first
	if err := ipam.Status(requestCtx); err != nil {
		err := fmt.Errorf("IPAM connectivity error: %w", err)
		logging.Errorf("%s", err.Error())
		return nil, err
	}

	overlappingrangestore, err := ipam.GetOverlappingRangeStore()
	if err != nil {
		err := fmt.Errorf("cannot get OverlappingRangeStore: %w", err)
		logging.Errorf("%s", err.Error())
		return nil, err
	}

	newips := make([]net.IPNet, 0, len(ipamConf.IPRanges))

	for _, ipRange := range ipamConf.IPRanges {
		newIP, err := s.allocateDeallocateFromRange(
			ctx,
			ipam,
			ipamConf,
			overlappingrangestore,
			ipRange,
			op,
			podRef,
			containerID,
			ifName,
		)
		if err != nil {
			ok := errors.As(err, new(allocate.AssignmentError))
			err := fmt.Errorf("cannot allocate IP from %v pool: %w", ipRange, err)
			if !ok || (ok && !ipamConf.SingleIP) {
				logging.Errorf("%s", err.Error())
				return nil, err
			}

			logging.Debugf("%s", err.Error())
			continue
		}

		if op == whereaboutstypes.Allocate && newIP.IP != nil {
			newips = append(newips, newIP)

			if ipamConf.SingleIP && len(newips) > 0 {
				logging.Debugf("Single IP is allocated from %v pool, stop iterating", ipRange)
				break
			}
		}
	}

	return newips, nil
}

func (s IPAMService) executeOperation(
	ctx context.Context,
	ipamConf whereaboutstypes.IPAMConfig,
	op whereaboutstypes.OperationType,
	poolNS, podRef, containerID, ifName string,
) ([]net.IPNet, error) {
	var newips []net.IPNet

	if ipamConf.PodName == "" {
		return newips, fmt.Errorf("IPAM client initialization error: no pod name")
	}

	ipam := &KubernetesIPAM{
		Client:      *s.client,
		Config:      ipamConf,
		ContainerID: containerID,
		IfName:      ifName,
		Namespace:   poolNS,
	}

	// setup leader election
	le, leader, deposed := newLeaderElector(ctx, s.client.clientSet, poolNS, ipam)
	var wg sync.WaitGroup
	wg.Add(2)

	stopM := make(chan struct{})
	result := make(chan error, 2)

	var err error
	go func() {
		defer wg.Done()
		for {
			select {
			case <-ctx.Done():
				err = fmt.Errorf("time limit exceeded while waiting to become leader")
				stopM <- struct{}{}
				return
			case <-leader:
				logging.Debugf("Elected as leader, do processing")
				newips, err = s.runOperation(
					ctx,
					ipam,
					ipamConf,
					op,
					poolNS,
					podRef,
					containerID,
					ifName,
				)
				stopM <- struct{}{}
				return
			case <-deposed:
				logging.Debugf("Deposed as leader, shutting down")
				result <- nil
				return
			}
		}
	}()

	go func() {
		defer wg.Done()
		res := make(chan error)
		leCtx, leCancel := context.WithCancel(context.Background())

		go func() {
			logging.Debugf("Started leader election")
			le.Run(leCtx)
			logging.Debugf("Finished leader election")
			res <- nil
		}()

		// wait for stop which tells us when IP allocation occurred or context deadline exceeded
		<-stopM
		// leCancel fn(leCtx)
		leCancel()
		result <- <-res
	}()

	wg.Wait()
	close(stopM)

	logging.Debugf("IPManagement: %v, %v", newips, err)

	return newips, err
}

func (s IPAMService) getNodeSlicedRangeAndPoolConfiguration(
	ctx context.Context,
	ipam *KubernetesIPAM,
	ipRange whereaboutstypes.RangeConfiguration,
	poolIdentifier PoolIdentifier,
) (whereaboutstypes.RangeConfiguration, PoolIdentifier, error) {
	hostname, err := getNodeName(ipam)
	if err != nil {
		return whereaboutstypes.RangeConfiguration{}, PoolIdentifier{}, fmt.Errorf("cannot get node hostname: %w", err)
	}

	poolIdentifier.NodeName = hostname

	nodeSliceRange, err := GetNodeSlicePoolRange(ctx, ipam, hostname)
	if err != nil {
		return whereaboutstypes.RangeConfiguration{}, PoolIdentifier{}, fmt.Errorf("cannot get node slice range for %s node: %w", hostname, err)
	}

	_, ipNet, err := net.ParseCIDR(nodeSliceRange)
	if err != nil {
		return whereaboutstypes.RangeConfiguration{}, PoolIdentifier{}, fmt.Errorf("cannot parse node slice cidr to net.IPNet: %w", err)
	}

	poolIdentifier.IpRange = nodeSliceRange
	pool := whereaboutstypes.Pool{
		IPNet:                   *ipNet,
		IncludeNetworkAddress:   ipRange.IncludeNetworkAddress,
		IncludeBroadcastAddress: ipRange.IncludeBroadcastAddress,
	}

	rangeStart, err := iphelpers.FirstUsableIP(pool)
	if err != nil {
		return whereaboutstypes.RangeConfiguration{}, PoolIdentifier{}, fmt.Errorf("cannot parse node slice cidr to range start: %w", err)
	}

	rangeEnd, err := iphelpers.LastUsableIP(pool)
	if err != nil {
		return whereaboutstypes.RangeConfiguration{}, PoolIdentifier{}, fmt.Errorf("cannot parse node slice cidr to range end: %w", err)
	}

	ipRange = whereaboutstypes.RangeConfiguration{
		Range: ipRange.Range,

		RangeStart:            rangeStart,
		IncludeNetworkAddress: ipRange.IncludeNetworkAddress,

		RangeEnd:                rangeEnd,
		IncludeBroadcastAddress: ipRange.IncludeBroadcastAddress,
	}

	return ipRange, poolIdentifier, nil
}
