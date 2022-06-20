//go:build windows
// +build windows

/*
Copyright 2018-2022 The Kubernetes Authors.

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
	"k8s.io/apimachinery/pkg/util/sets"
	"k8s.io/klog/v2"
	//	"k8s.io/kubernetes/pkg/proxy"
	"net"
	"strconv"
)

// internal struct for endpoints information
type endpointsInfo struct {
	ip              string
	port            uint16
	isLocal         bool
	macAddress      string
	hnsID           string
	refCount        *uint16
	providerAddress string
	hns             HCNUtils
	name            string

	// conditions
	ready       bool
	serving     bool
	terminating bool
}

// String is part of proxy.Endpoint interface.
func (info *endpointsInfo) String() string {
	return net.JoinHostPort(info.ip, strconv.Itoa(int(info.port)))
}

// GetIsLocal is part of proxy.Endpoint interface.
func (info *endpointsInfo) GetIsLocal() bool {
	return info.isLocal
}

// IsReady returns true if an endpoint is ready and not terminating.
func (info *endpointsInfo) IsReady() bool {
	return info.ready
}

// IsServing returns true if an endpoint is ready, regardless of it's terminating state.
func (info *endpointsInfo) IsServing() bool {
	return info.serving
}

// IsTerminating returns true if an endpoint is terminating.
func (info *endpointsInfo) IsTerminating() bool {
	return info.terminating
}

// GetZoneHint returns the zone hint for the endpoint.
func (info *endpointsInfo) GetZoneHints() sets.String {
	return sets.String{}
}

// IP returns just the IP part of the endpoint, it's a part of proxy.Endpoint interface.
func (info *endpointsInfo) IP() string {
	return info.ip
}

// Port returns just the Port part of the endpoint.
func (info *endpointsInfo) Port() (int, error) {
	return int(info.port), nil
}

// Equal is part of proxy.Endpoint interface.
func (info *endpointsInfo) Equal(other Endpoint) bool {
	return info.String() == other.String() && info.GetIsLocal() == other.GetIsLocal()
}

// GetNodeName returns the NodeName for this endpoint.
func (info *endpointsInfo) GetNodeName() string {
	return ""
}

// GetZone returns the Zone for this endpoint.
func (info *endpointsInfo) GetZone() string {
	return ""
}

func (info *endpointsInfo) GetTopology() map[string]string {
	return map[string]string{}
}

func (info *endpointsInfo) Cleanup() {
    klog.V(3).InfoS("Endpoint cleanup", "endpointsInfo", info)
    if !info.GetIsLocal() && info.refCount != nil {
        *info.refCount--

        // Remove the remote hns endpoint, if no service is referring it
        // Never delete a Local Endpoint. Local Endpoints are already created by other entities.
        // Remove only remote endpoints created by this service
        if *info.refCount <= 0 && !info.GetIsLocal() {
            klog.V(4).InfoS("Removing endpoints, since no one is referencing it", "endpoint", info)
            err := info.hns.deleteEndpoint(info.hnsID)
            if err == nil {
                info.hnsID = ""
            } else {
                klog.ErrorS(err, "Endpoint deletion failed", "ip", info.IP())
            }
        }

        info.refCount = nil
    }
}
