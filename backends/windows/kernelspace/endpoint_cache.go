package kernelspace

import (
	"encoding/json"
	"github.com/pkg/errors"
	"k8s.io/klog/v2"
	"sigs.k8s.io/kpng/api/localnetv1"
)

type kpngEndpointCache struct {
	endpoints map[string]*windowsEndpoint
}

func newKpngEndpointCache() *kpngEndpointCache {
	return &kpngEndpointCache{endpoints: map[string]*windowsEndpoint{}}
}

func (k *kpngEndpointCache) storeEndpoint(ep localnetv1.Endpoint, we *windowsEndpoint) {
	klog.V(0).InfoS("Serializing ep and searching hnsIDs for it... %v %v", ep, we)
	key := getEndpointKey(&ep)
	if _, ok := k.endpoints[key]; ok {
		panic(any(errors.Errorf("TODO -- key conflict found; should we overwrite the existing endpoint entry?")))
	}
	k.endpoints[key] = we
}

// important function that services the ep := epInfo code... we need to look up the corresponding
// hns endpoint when that happens.
func (k *kpngEndpointCache) findEndpoint(ep *localnetv1.Endpoint) (*windowsEndpoint, bool) {
	key := getEndpointKey(ep)
	if value, ok := k.endpoints[key]; ok {
		return value, true
	}
	return nil, false
}

func (k *kpngEndpointCache) removeEndpoint(ep *localnetv1.Endpoint) (*windowsEndpoint, bool) {
	key := getEndpointKey(ep)
	if value, ok := k.endpoints[key]; ok {
		delete(k.endpoints, key)
		return value, true
	}
	return nil, false
}

func getEndpointKey(endpoint *localnetv1.Endpoint) string {
	klog.V(0).InfoS("Serializing endpoint to json: %v", endpoint)
	bytes, err := json.Marshal(endpoint)
	if err != nil {
		klog.Errorf("unable to serialize endpoint to json: %s", err)
		return ""
	}
	return string(bytes)
}

// TODO figure out what this is supposed to do
func kpngGetHNS(endpoint *windowsEndpoint) string {
	panic(any(errors.Errorf("TODO -- unimplemented placeholder")))
}
