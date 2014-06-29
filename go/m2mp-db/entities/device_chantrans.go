package entities

import (
	"errors"
	"fmt"
	db "github.com/fclairamb/m2mp/go/m2mp-db"
	"regexp"
	"sort"
	"strconv"
	"strings"
)

type DeviceChannelTrans struct {
	Device  *Device
	matches map[string]*string
}

func NewDeviceChannelTrans(d *Device) *DeviceChannelTrans {
	return &DeviceChannelTrans{Device: d, matches: make(map[string]*string)}
}

const CHANNEL_TRANSLATION_SUBNODE = "channel-translation"

func (d *Device) hasChannelTranslationNode() bool {
	return d.Node.GetChild(CHANNEL_TRANSLATION_SUBNODE).Exists()
}

func (d *Device) getChannelTranslationNode() *db.RegistryNode {
	return d.Node.GetChild("channel-translation").Check()
}

func computeTarget(rules map[string]string, channel string) string {
	keys := make([]string, 0, len(rules))
	for k := range rules {
		keys = append(keys, k)
	}
	sort.Strings(keys)

	for _, k := range keys {
		pattern := strings.SplitN(k, "_", 2)[1]
		target := rules[k]

		if matched, _ := regexp.Match(pattern, []byte(channel)); matched {
			return target
		}
	}
	return ""
}

func prepareRules(rules map[string]string, n *db.RegistryNode, multiplier, increment int) {
	for k, v := range n.Values() {
		spl := strings.SplitN(k, "_", 2)
		if len(spl) < 2 {
			n.DelValue(k)
			continue
		}
		priority, _ := strconv.Atoi(spl[0])
		rule := spl[1]
		rules[fmt.Sprintf("%05d_%s", priority*multiplier+increment, rule)] = v
	}
}

func (d *DeviceChannelTrans) computeTarget(channel string) string {
	rules := make(map[string]string)

	// What we are doing here is to merge the specific and default rules
	// by giving a higher priority to the specific one.

	// It feels a little bit complex but I don't really see an easier pattern here

	// We search for the current device
	if d.Device.hasChannelTranslationNode() {
		prepareRules(rules, d.Device.getChannelTranslationNode(), 2, 1)
	}

	// And then the default device
	prepareRules(rules, NewDeviceDefault().getChannelTranslationNode(), 2, 0)

	//fmt.Printf("Rules are: %s\n", rules)

	return computeTarget(rules, channel)
}

func (d *DeviceChannelTrans) GetTarget(channel string) *string {
	target := d.matches[channel]

	// If it's not in the cache...
	if target == nil {
		// We compute it
		computed := d.computeTarget(channel)
		target = &computed
		d.matches[channel] = target
	}

	if *target == "" {
		return nil
	} else {
		return target
	}
}

func (d *DeviceChannelTrans) SetTarget(priority int, channel, target string) error {

	if priority < 0 || priority > 100 {
		return errors.New("The priority must be between 0 and 100")
	}

	// We check that the regexp can be compiled before storing it
	if _, err := regexp.Compile(channel); err != nil {
		return err
	}

	d.Device.getChannelTranslationNode().SetValue(fmt.Sprintf("%04d_%s", priority, channel), target)

	// Optimization is not my concern here
	d.matches = make(map[string]*string)

	return nil
}
