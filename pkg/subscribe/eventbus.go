package subscribe

import (
	"fmt"
	"sync"
)

// EventBus provides an in-memory pub/sub mechanism for service and config changes.
type EventBus struct {
	mu          sync.RWMutex
	serviceSubs map[string][]chan struct{} // key: namespace#group#service
	configSubs  map[string][]chan struct{} // key: namespace#group#dataId
}

func NewEventBus() *EventBus {
	return &EventBus{
		serviceSubs: make(map[string][]chan struct{}),
		configSubs:  make(map[string][]chan struct{}),
	}
}

func serviceKey(namespaceID, groupName, name string) string {
	return fmt.Sprintf("%s#%s#%s", namespaceID, groupName, name)
}

// --- Service change events ---

// PublishServiceChange notifies all subscribers of a service change.
func (eb *EventBus) PublishServiceChange(namespaceID, groupName, serviceName string) {
	key := serviceKey(namespaceID, groupName, serviceName)
	eb.mu.RLock()
	subs := eb.serviceSubs[key]
	eb.mu.RUnlock()
	for _, ch := range subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// SubscribeService registers a subscriber for service changes and returns
// a channel that receives notifications.
func (eb *EventBus) SubscribeService(namespaceID, groupName, serviceName string) chan struct{} {
	key := serviceKey(namespaceID, groupName, serviceName)
	ch := make(chan struct{}, 1)
	eb.mu.Lock()
	eb.serviceSubs[key] = append(eb.serviceSubs[key], ch)
	eb.mu.Unlock()
	return ch
}

// UnsubscribeService removes a subscriber channel.
func (eb *EventBus) UnsubscribeService(namespaceID, groupName, serviceName string, ch chan struct{}) {
	key := serviceKey(namespaceID, groupName, serviceName)
	eb.mu.Lock()
	defer eb.mu.Unlock()
	subs := eb.serviceSubs[key]
	for i, s := range subs {
		if s == ch {
			eb.serviceSubs[key] = append(subs[:i], subs[i+1:]...)
			close(ch)
			return
		}
	}
}

// --- Config change events ---

// PublishConfigChange notifies all subscribers of a config change.
func (eb *EventBus) PublishConfigChange(namespaceID, groupName, dataID string) {
	key := serviceKey(namespaceID, groupName, dataID)
	eb.mu.RLock()
	subs := eb.configSubs[key]
	eb.mu.RUnlock()
	for _, ch := range subs {
		select {
		case ch <- struct{}{}:
		default:
		}
	}
}

// SubscribeConfig registers a subscriber for config changes.
func (eb *EventBus) SubscribeConfig(namespaceID, groupName, dataID string) chan struct{} {
	key := serviceKey(namespaceID, groupName, dataID)
	ch := make(chan struct{}, 1)
	eb.mu.Lock()
	eb.configSubs[key] = append(eb.configSubs[key], ch)
	eb.mu.Unlock()
	return ch
}

// SubscriberInfo describes a service/config key with its subscriber count.
type SubscriberInfo struct {
	NamespaceID   string `json:"namespaceId"`
	GroupName     string `json:"groupName"`
	ServiceName   string `json:"serviceName"`
	SubscriberCnt int    `json:"subscriberCount"`
}

// GetServiceSubscribers returns all service keys with their subscriber counts.
func (eb *EventBus) GetServiceSubscribers() []SubscriberInfo {
	eb.mu.RLock()
	defer eb.mu.RUnlock()
	var list []SubscriberInfo
	for key, subs := range eb.serviceSubs {
		if len(subs) == 0 {
			continue
		}
		ns, grp, svc := parseKey(key)
		list = append(list, SubscriberInfo{
			NamespaceID:   ns,
			GroupName:     grp,
			ServiceName:   svc,
			SubscriberCnt: len(subs),
		})
	}
	return list
}

func parseKey(key string) (ns, group, name string) {
	parts := make([]string, 0, 3)
	cur := ""
	for _, c := range key {
		if c == '#' {
			parts = append(parts, cur)
			cur = ""
		} else {
			cur += string(c)
		}
	}
	parts = append(parts, cur)
	if len(parts) >= 3 {
		return parts[0], parts[1], parts[2]
	}
	return "", "", key
}

// UnsubscribeConfig removes a subscriber channel.
func (eb *EventBus) UnsubscribeConfig(namespaceID, groupName, dataID string, ch chan struct{}) {
	key := serviceKey(namespaceID, groupName, dataID)
	eb.mu.Lock()
	defer eb.mu.Unlock()
	subs := eb.configSubs[key]
	for i, s := range subs {
		if s == ch {
			eb.configSubs[key] = append(subs[:i], subs[i+1:]...)
			close(ch)
			return
		}
	}
}
