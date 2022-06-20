//go:build windows
// +build windows

/*
Copyright 2017-2022 The Kubernetes Authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

    http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

package kernelspace

import (
	"strings"
	"sync/atomic"
	"time"

	v1 "k8s.io/api/core/v1"
	"k8s.io/apimachinery/pkg/util/wait"
	"k8s.io/klog/v2"
	netutils "k8s.io/utils/net"
)

// Sync is called to synchronize the Proxier state to hns as soon as possible.
func (proxier *Proxier) Sync() {
	//	if Proxier.healthzServer != nil {
	//		Proxier.healthzServer.QueuedUpdate()
	//	}

	// TODO commenting out metrics, Jay to fix , figure out how to  copy these later, avoiding pkg/proxy imports
	// metrics.SyncProxyRulesLastQueuedTimestamp.SetToCurrentTime()

	klog.V(0).InfoS("proxier_sync.Sync ->")
	proxier.syncRunner.Run()
}

// SyncLoop runs periodic work.  This is expected to run as a goroutine or as the main loop of the app.  It does not return.
func (proxier *Proxier) SyncLoop() {
	// Update healthz timestamp at beginning in case Sync() never succeeds.
	//	if proxier.healthzServer != nil {
	//		proxier.healthzServer.Updated()
	//	}
	// synthesize "last change queued" time as the informers are syncing.
	//	metrics.SyncProxyRulesLastQueuedTimestamp.SetToCurrentTime()
	proxier.syncRunner.Loop(wait.NeverStop)
}

func (proxier *Proxier) isInitialized() bool {
	return atomic.LoadInt32(&proxier.initialized) > 0
}

func (proxier *Proxier) cleanupAllPolicies() {
	for svcName, svcPortMap := range proxier.serviceMap {
		for _, svc := range svcPortMap {

			svcInfo, ok := svc.(*serviceInfo)
			if !ok {
				klog.ErrorS(nil, "Failed to cast serviceInfo", "serviceName", svcName)
				continue
			}
			endpoints := proxier.endpointsMap[svcName]
			for _, e := range *endpoints {
				svcInfo.cleanupAllPolicies(e)
			}
		}
	}
}

type loadBalancerInfo struct {
	hnsID string
}

type loadBalancerIdentifier struct {
	protocol       uint16
	internalPort   uint16
	externalPort   uint16
	vip            string
	endpointsCount int
}

type loadBalancerFlags struct {
	isILB           bool
	isDSR           bool
	localRoutedVIP  bool
	useMUX          bool
	preserveDIP     bool
	sessionAffinity bool
	isIPv6          bool
}

type hnsNetworkInfo struct {
	name          string
	id            string
	networkType   string
	remoteSubnets []*remoteSubnetInfo
}

type remoteSubnetInfo struct {
	destinationPrefix string
	isolationID       uint16
	providerAddress   string
	drMacAddress      string
}

const NETWORK_TYPE_OVERLAY = "overlay"
const NETWORK_TYPE_L2BRIDGE = "L2Bridge"

// This is where all of the hns save/restore calls happen.
// assumes Proxier.mu is held
func (proxier *Proxier) syncProxyRules() {
	proxier.mu.Lock()
	defer proxier.mu.Unlock()

	// don't sync rules till we've received services and endpoints
	if !proxier.isInitialized() {
		klog.V(2).InfoS("Not syncing hns until Services and Endpoints have been received from master")
		return
	}

	// Keep track of how long syncs take.
	start := time.Now()
	defer func() {
		//metrics.SyncProxyRulesLatency.Observe(metrics.SinceInSeconds(start))
		klog.V(4).InfoS("Syncing proxy rules complete", "elapsed", time.Since(start))
	}()

	hnsNetworkName := proxier.network.name
	hns := proxier.hns

	prevNetworkID := proxier.network.id
	updatedNetwork, err := hns.getNetworkByName(hnsNetworkName)
	if updatedNetwork == nil || updatedNetwork.id != prevNetworkID || isNetworkNotFoundError(err) {
		klog.InfoS("The HNS network is not present or has changed since the last sync, please check the CNI deployment", "hnsNetworkName", hnsNetworkName)
		proxier.cleanupAllPolicies()
		if updatedNetwork != nil {
			proxier.network = *updatedNetwork
		}
		return
	}

	// We assume that if this was called, we really want to sync them,
	// even if nothing changed in the meantime. In other words, callers are
	// responsible for detecting no-op changes and not calling this function.
	serviceUpdateResult := proxier.serviceMap.Update(proxier.serviceChanges)
	endpointUpdateResult := proxier.endpointsMap.Update(proxier.endpointsChanges)

	staleServices := serviceUpdateResult.UDPStaleClusterIP
	// merge stale services gathered from updateEndpointsMap
	for _, svcPortName := range endpointUpdateResult.StaleServiceNames {
		klog.InfoS("echo %v", svcPortName)
		//if svcInfo, ok := proxier.serviceMap[svcPortName]; ok && svcInfo != nil && svcInfo.Protocol() == v1.ProtocolUDP {
		//	klog.V(2).InfoS("Stale udp service", "servicePortName", svcPortName, "clusterIP", svcInfo.ClusterIP())
		//	staleServices.Insert(svcInfo.ClusterIP().String())
		//}
	}
	// Query HNS for endpoints and load balancers
	queriedEndpoints, err := hns.getAllEndpointsByNetwork(hnsNetworkName)
	if err != nil {
		klog.ErrorS(err, "Querying HNS for endpoints failed")
		return
	}
	if queriedEndpoints == nil {
		klog.V(4).InfoS("No existing endpoints found in HNS")
		queriedEndpoints = make(map[string]*(endpointsInfo))
	}
	queriedLoadBalancers, err := hns.getAllLoadBalancers()
	if queriedLoadBalancers == nil {
		klog.V(4).InfoS("No existing load balancers found in HNS")
		queriedLoadBalancers = make(map[loadBalancerIdentifier]*(loadBalancerInfo))
	}
	if err != nil {
		klog.ErrorS(err, "Querying HNS for load balancers failed")
		return
	}
	if strings.EqualFold(proxier.network.networkType, NETWORK_TYPE_OVERLAY) {
		if _, ok := queriedEndpoints[proxier.sourceVip]; !ok {
			_, err = newSourceVIP(hns, hnsNetworkName, proxier.sourceVip, proxier.hostMac, proxier.nodeIP.String())
			if err != nil {
				klog.ErrorS(err, "Source Vip endpoint creation failed")
				return
			}
		}
	}

	klog.V(3).InfoS("Syncing Policies")

	// Program HNS by adding corresponding policies for each service.
	for svcName, svcPortMap := range proxier.serviceMap {
		for _, svc := range svcPortMap {
			svcInfo, ok := svc.(*serviceInfo)
			if !ok {
				klog.ErrorS(nil, "Failed to cast serviceInfo", "serviceName", svcName)
				continue
			}

			if svcInfo.policyApplied {
				klog.V(4).InfoS("Policy already applied", "serviceInfo", svcInfo)
				continue
			}

			if strings.EqualFold(proxier.network.networkType, NETWORK_TYPE_OVERLAY) {
				serviceVipEndpoint := queriedEndpoints[svcInfo.ClusterIP().String()]
				if serviceVipEndpoint == nil {
					klog.V(4).InfoS("No existing remote endpoint", "IP", svcInfo.ClusterIP())
					hnsEndpoint := &endpointsInfo{
						ip:              svcInfo.ClusterIP().String(),
						isLocal:         false,
						macAddress:      proxier.hostMac,
						providerAddress: proxier.nodeIP.String(),
					}

					newHnsEndpoint, err := hns.createEndpoint(hnsEndpoint, hnsNetworkName)
					if err != nil {
						klog.ErrorS(err, "Remote endpoint creation failed for service VIP")
						continue
					}

					newHnsEndpoint.refCount = proxier.endPointsRefCount.getRefCount(newHnsEndpoint.hnsID)
					*newHnsEndpoint.refCount++
					svcInfo.remoteEndpoint = newHnsEndpoint
					// store newly created endpoints in queriedEndpoints
					queriedEndpoints[newHnsEndpoint.hnsID] = newHnsEndpoint
					queriedEndpoints[newHnsEndpoint.ip] = newHnsEndpoint
				}
			}

			var hnsEndpoints []endpointsInfo
			var hnsLocalEndpoints []endpointsInfo
			klog.V(4).InfoS("Applying Policy", "serviceInfo", svcName)
			// Create Remote endpoints for every endpoint, corresponding to the service
			containsPublicIP := false
			containsNodeIP := false

			endpoints := proxier.endpointsMap[svcName]
			for _, e := range *endpoints {
    				ep := &endpointsInfo{
    				    ip:		e.IPs.First(),
    				    isLocal:	e.Local,
    				    hns: 	proxier.hns,
				    ready:	true,
				    serving:    true,  // TODO same as above?
    				}
 
				if !ok {
					klog.ErrorS(nil, "Failed to cast endpointsInfo", "serviceName", svcName)
					continue
				}

				if !ep.IsReady() {
					continue
				}
				var newHnsEndpoint *endpointsInfo
				hnsNetworkName := proxier.network.name
				var err error

				// targetPort is zero if it is specified as a name in port.TargetPort, so the real port should be got from endpoints.
				// Note that hcsshim.AddLoadBalancer() doesn't support endpoints with different ports, so only port from first endpoint is used.
				// TODO(feiskyer): add support of different endpoint ports after hcsshim.AddLoadBalancer() add that.
				if svcInfo.targetPort == 0 {
					svcInfo.targetPort = int(ep.port)
				}
				// There is a bug in Windows Server 2019 that can cause two endpoints to be created with the same IP address, so we need to check using endpoint ID first.
				// TODO: Remove lookup by endpoint ID, and use the IP address only, so we don't need to maintain multiple keys for lookup.
				if len(ep.hnsID) > 0 {
					newHnsEndpoint = queriedEndpoints[ep.hnsID]
				}

				if newHnsEndpoint == nil {
					// First check if an endpoint resource exists for this IP, on the current host
					// A Local endpoint could exist here already
					// A remote endpoint was already created and proxy was restarted
					newHnsEndpoint = queriedEndpoints[ep.IP()]
				}

				if newHnsEndpoint == nil {
					if ep.GetIsLocal() {
						klog.ErrorS(err, "Local endpoint not found: on network", "ip", ep.IP(), "hnsNetworkName", hnsNetworkName)
						continue
					}

					if strings.EqualFold(proxier.network.networkType, NETWORK_TYPE_OVERLAY) {
						klog.InfoS("Updating network to check for new remote subnet policies", "networkName", proxier.network.name)
						networkName := proxier.network.name
						updatedNetwork, err := hns.getNetworkByName(networkName)
						if err != nil {
							klog.ErrorS(err, "Unable to find HNS Network specified, please check network name and CNI deployment", "hnsNetworkName", hnsNetworkName)
							proxier.cleanupAllPolicies()
							return
						}
						proxier.network = *updatedNetwork
						providerAddress := proxier.network.findRemoteSubnetProviderAddress(ep.IP())
						if len(providerAddress) == 0 {
							klog.InfoS("Could not find provider address, assuming it is a public IP", "IP", ep.IP())
							providerAddress = proxier.nodeIP.String()
						}

						hnsEndpoint := &endpointsInfo{
							ip:              ep.ip,
							isLocal:         false,
							macAddress:      conjureMac("02-11", netutils.ParseIPSloppy(ep.ip)),
							providerAddress: providerAddress,
						}

						newHnsEndpoint, err = hns.createEndpoint(hnsEndpoint, hnsNetworkName)
						if err != nil {
							klog.ErrorS(err, "Remote endpoint creation failed", "endpointsInfo", hnsEndpoint)
							continue
						}
					} else {

						hnsEndpoint := &endpointsInfo{
							ip:         ep.ip,
							isLocal:    false,
							macAddress: ep.macAddress,
						}

						newHnsEndpoint, err = hns.createEndpoint(hnsEndpoint, hnsNetworkName)
						if err != nil {
							klog.ErrorS(err, "Remote endpoint creation failed")
							continue
						}
					}
				}
				// For Overlay networks 'SourceVIP' on an Load balancer Policy can either be chosen as
				// a) Source VIP configured on kube-proxy (or)
				// b) Node IP of the current node
				//
				// For L2Bridge network the Source VIP is always the NodeIP of the current node and the same
				// would be configured on kube-proxy as SourceVIP
				//
				// The logic for choosing the SourceVIP in Overlay networks is based on the backend endpoints:
				// a) Endpoints are any IP's outside the cluster ==> Choose NodeIP as the SourceVIP
				// b) Endpoints are IP addresses of a remote node => Choose NodeIP as the SourceVIP
				// c) Everything else (Local POD's, Remote POD's, Node IP of current node) ==> Choose the configured SourceVIP
				if strings.EqualFold(proxier.network.networkType, NETWORK_TYPE_OVERLAY) && !ep.GetIsLocal() {
					providerAddress := proxier.network.findRemoteSubnetProviderAddress(ep.IP())

					isNodeIP := (ep.IP() == providerAddress)
					isPublicIP := (len(providerAddress) == 0)
					klog.InfoS("Endpoint on overlay network", "ip", ep.IP(), "hnsNetworkName", hnsNetworkName, "isNodeIP", isNodeIP, "isPublicIP", isPublicIP)

					containsNodeIP = containsNodeIP || isNodeIP
					containsPublicIP = containsPublicIP || isPublicIP
				}

				// Save the hnsId for reference
				klog.V(1).InfoS("Hns endpoint resource", "endpointsInfo", newHnsEndpoint)

				hnsEndpoints = append(hnsEndpoints, *newHnsEndpoint)
				if newHnsEndpoint.GetIsLocal() {
					hnsLocalEndpoints = append(hnsLocalEndpoints, *newHnsEndpoint)
				} else {
					// We only share the refCounts for remote endpoints
					ep.refCount = proxier.endPointsRefCount.getRefCount(newHnsEndpoint.hnsID)
					*ep.refCount++
				}

				ep.hnsID = newHnsEndpoint.hnsID

				klog.V(3).InfoS("Endpoint resource found", "endpointsInfo", ep)
			}

			klog.V(3).InfoS("Associated endpoints for service", "endpointsInfo", hnsEndpoints, "serviceName", svcName)

			if len(svcInfo.hnsID) > 0 {
				// This should not happen
				klog.InfoS("Load Balancer already exists -- Debug ", "hnsID", svcInfo.hnsID)
			}

			if len(hnsEndpoints) == 0 {
				klog.ErrorS(nil, "Endpoint information not available for service, not applying any policy", "serviceName", svcName)
				continue
			}

			klog.V(4).InfoS("Trying to apply Policies for service", "serviceInfo", svcInfo)
			var hnsLoadBalancer *loadBalancerInfo
			var sourceVip = proxier.sourceVip
			if containsPublicIP || containsNodeIP {
				sourceVip = proxier.nodeIP.String()
			}

			sessionAffinityClientIP := svcInfo.SessionAffinityType() == v1.ServiceAffinityClientIP
			if sessionAffinityClientIP && !proxier.supportedFeatures.SessionAffinity {
				klog.InfoS("Session Affinity is not supported on this version of Windows")
			}

			hnsLoadBalancer, err := hns.getLoadBalancer(
				hnsEndpoints,
				loadBalancerFlags{isDSR: proxier.isDSR, isIPv6: proxier.isIPv6Mode, sessionAffinity: sessionAffinityClientIP},
				sourceVip,
				svcInfo.ClusterIP().String(),
				Enum(svcInfo.Protocol()),
				uint16(svcInfo.targetPort),
				uint16(svcInfo.Port()),
				queriedLoadBalancers,
			)
			if err != nil {
				klog.ErrorS(err, "Policy creation failed")
				continue
			}

			svcInfo.hnsID = hnsLoadBalancer.hnsID
			klog.V(3).InfoS("Hns LoadBalancer resource created for cluster ip resources", "clusterIP", svcInfo.ClusterIP(), "hnsID", hnsLoadBalancer.hnsID)

			// If nodePort is specified, user should be able to use nodeIP:nodePort to reach the backend endpoints
			if svcInfo.NodePort() > 0 {
				// If the preserve-destination service annotation is present, we will disable routing mesh for NodePort.
				// This means that health services can use Node Port without falsely getting results from a different node.
				nodePortEndpoints := hnsEndpoints
				if svcInfo.preserveDIP || svcInfo.localTrafficDSR {
					nodePortEndpoints = hnsLocalEndpoints
				}

				if len(nodePortEndpoints) > 0 {
					hnsLoadBalancer, err := hns.getLoadBalancer(
						nodePortEndpoints,
						loadBalancerFlags{isDSR: svcInfo.localTrafficDSR, localRoutedVIP: true, sessionAffinity: sessionAffinityClientIP, isIPv6: proxier.isIPv6Mode},
						sourceVip,
						"",
						Enum(svcInfo.Protocol()),
						uint16(svcInfo.targetPort),
						uint16(svcInfo.NodePort()),
						queriedLoadBalancers,
					)
					if err != nil {
						klog.ErrorS(err, "Policy creation failed")
						continue
					}

					svcInfo.nodePorthnsID = hnsLoadBalancer.hnsID
					klog.V(3).InfoS("Hns LoadBalancer resource created for nodePort resources", "clusterIP", svcInfo.ClusterIP(), "nodeport", svcInfo.NodePort(), "hnsID", hnsLoadBalancer.hnsID)
				} else {
					klog.V(3).InfoS("Skipped creating Hns LoadBalancer for nodePort resources", "clusterIP", svcInfo.ClusterIP(), "nodeport", svcInfo.NodePort(), "hnsID", hnsLoadBalancer.hnsID)
				}
			}

			// Create a Load Balancer Policy for each external IP
			for _, externalIP := range svcInfo.externalIPs {
				// Disable routing mesh if ExternalTrafficPolicy is set to local
				externalIPEndpoints := hnsEndpoints
				if svcInfo.localTrafficDSR {
					externalIPEndpoints = hnsLocalEndpoints
				}

				if len(externalIPEndpoints) > 0 {
					// Try loading existing policies, if already available
					hnsLoadBalancer, err = hns.getLoadBalancer(
						externalIPEndpoints,
						loadBalancerFlags{isDSR: svcInfo.localTrafficDSR, sessionAffinity: sessionAffinityClientIP, isIPv6: proxier.isIPv6Mode},
						sourceVip,
						externalIP.ip,
						Enum(svcInfo.Protocol()),
						uint16(svcInfo.targetPort),
						uint16(svcInfo.Port()),
						queriedLoadBalancers,
					)
					if err != nil {
						klog.ErrorS(err, "Policy creation failed")
						continue
					}
					externalIP.hnsID = hnsLoadBalancer.hnsID
					klog.V(3).InfoS("Hns LoadBalancer resource created for externalIP resources", "externalIP", externalIP, "hnsID", hnsLoadBalancer.hnsID)
				} else {
					klog.V(3).InfoS("Skipped creating Hns LoadBalancer for externalIP resources", "externalIP", externalIP, "hnsID", hnsLoadBalancer.hnsID)
				}
			}
			// Create a Load Balancer Policy for each loadbalancer ingress
			for _, lbIngressIP := range svcInfo.loadBalancerIngressIPs {
				// Try loading existing policies, if already available
				lbIngressEndpoints := hnsEndpoints
				if svcInfo.preserveDIP || svcInfo.localTrafficDSR {
					lbIngressEndpoints = hnsLocalEndpoints
				}

				if len(lbIngressEndpoints) > 0 {
					hnsLoadBalancer, err := hns.getLoadBalancer(
						lbIngressEndpoints,
						loadBalancerFlags{isDSR: svcInfo.preserveDIP || svcInfo.localTrafficDSR, useMUX: svcInfo.preserveDIP, preserveDIP: svcInfo.preserveDIP, sessionAffinity: sessionAffinityClientIP, isIPv6: proxier.isIPv6Mode},
						sourceVip,
						lbIngressIP.ip,
						Enum(svcInfo.Protocol()),
						uint16(svcInfo.targetPort),
						uint16(svcInfo.Port()),
						queriedLoadBalancers,
					)
					if err != nil {
						klog.ErrorS(err, "Policy creation failed")
						continue
					}
					lbIngressIP.hnsID = hnsLoadBalancer.hnsID
					klog.V(3).InfoS("Hns LoadBalancer resource created for loadBalancer Ingress resources", "lbIngressIP", lbIngressIP)
				} else {
					klog.V(3).InfoS("Skipped creating Hns LoadBalancer for loadBalancer Ingress resources", "lbIngressIP", lbIngressIP)
				}
				lbIngressIP.hnsID = hnsLoadBalancer.hnsID
				klog.V(3).InfoS("Hns LoadBalancer resource created for loadBalancer Ingress resources", "lbIngressIP", lbIngressIP)

			}
			svcInfo.policyApplied = true
			klog.V(2).InfoS("Policy successfully applied for service", "serviceInfo", svcInfo)
		}
	}

	if proxier.healthzServer != nil {
		proxier.healthzServer.Updated()
	}
	//metrics.SyncProxyRulesLastTimestamp.SetToCurrentTime()

	// Update service healthchecks.  The endpoints list might include services that are
	// not "OnlyLocal", but the services list will not, and the serviceHealthServer
	// will just drop those endpoints.
	if err := proxier.serviceHealthServer.SyncServices(serviceUpdateResult.HCServiceNodePorts); err != nil {
		klog.ErrorS(err, "Error syncing healthcheck services")
	}
	if err := proxier.serviceHealthServer.SyncEndpoints(endpointUpdateResult.HCEndpointsLocalIPSize); err != nil {
		klog.ErrorS(err, "Error syncing healthcheck endpoints")
	}

	// Finish housekeeping.
	// TODO: these could be made more consistent.
	for _, svcIP := range staleServices.UnsortedList() {
		// TODO : Check if this is required to cleanup stale services here
		klog.V(5).InfoS("Pending delete stale service IP connections", "IP", svcIP)
	}

	// remove stale endpoint refcount entries
	for hnsID, referenceCount := range proxier.endPointsRefCount {
		if *referenceCount <= 0 {
			delete(proxier.endPointsRefCount, hnsID)
		}
	}
}
