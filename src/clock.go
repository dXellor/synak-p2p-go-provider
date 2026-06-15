package main

// VectorClock serializes to {"node_id": "...", "clock": {"n1": 3, "n2": 1}}
// matching Python's VectorClock.to_dict() format exactly.
type VectorClock struct {
	NodeID string         `json:"node_id"`
	Clock  map[string]int `json:"clock"`
}

func NewClock(nodeID string) VectorClock {
	return VectorClock{NodeID: nodeID, Clock: make(map[string]int)}
}

// ClockFromDict deserializes from the map[string]any that JSON unmarshal produces.
// Numbers arrive as float64; we convert to int.
func ClockFromDict(data map[string]any, nodeID string) VectorClock {
	clk := VectorClock{NodeID: nodeID, Clock: make(map[string]int)}
	raw, ok := data["clock"]
	if !ok {
		return clk
	}
	m, ok := raw.(map[string]any)
	if !ok {
		return clk
	}
	for k, v := range m {
		switch val := v.(type) {
		case float64:
			clk.Clock[k] = int(val)
		case int:
			clk.Clock[k] = val
		}
	}
	return clk
}

func (c *VectorClock) Increment() {
	c.Clock[c.NodeID]++
}

// Update merges other into c, taking the component-wise maximum (Bayou receive rule).
func (c *VectorClock) Update(other VectorClock) {
	for node, ts := range other.Clock {
		if ts > c.Clock[node] {
			c.Clock[node] = ts
		}
	}
}

func (c VectorClock) HappensBefore(other VectorClock) bool {
	atLeastOneLess := false
	for node, s := range c.Clock {
		o := other.Clock[node]
		if s > o {
			return false
		}
		if s < o {
			atLeastOneLess = true
		}
	}
	for node, o := range other.Clock {
		if _, seen := c.Clock[node]; !seen {
			if o > 0 {
				atLeastOneLess = true
			}
		}
	}
	return atLeastOneLess
}

func (c VectorClock) Equals(other VectorClock) bool {
	allNodes := make(map[string]struct{})
	for k := range c.Clock {
		allNodes[k] = struct{}{}
	}
	for k := range other.Clock {
		allNodes[k] = struct{}{}
	}
	for node := range allNodes {
		if c.Clock[node] != other.Clock[node] {
			return false
		}
	}
	return true
}

func (c VectorClock) ToDict() map[string]any {
	clk := make(map[string]int, len(c.Clock))
	for k, v := range c.Clock {
		clk[k] = v
	}
	return map[string]any{"node_id": c.NodeID, "clock": clk}
}
