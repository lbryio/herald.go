package schema

import (
	"fmt"
	"github.com/lbryio/hub/util"
	"log"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf16"
)

type PathSegment struct {
	Name        string
	ClaimId     string
	AmountOrder int
}

type URL struct {
	Stream  *PathSegment
	Channel *PathSegment
}

func (ps *PathSegment) Normalized() string {
	return util.Normalize(ps.Name)
}

func (ps *PathSegment) IsShortID() bool {
	return len(ps.ClaimId) < 40
}

func (ps *PathSegment) IsFullID() bool {
	return len(ps.ClaimId) == 40
}

func (ps *PathSegment) String() string {
	if ps == nil {
		return ""
	}
	if ps.ClaimId != "" {
		return ps.Name + ":" + ps.ClaimId
	} else if ps.AmountOrder >= 0 {
		return fmt.Sprintf("%s$%d", ps.Name, ps.AmountOrder)
	}
	return ps.Name
}

func (url *URL) HasChannel() bool {
	return url.Channel != nil
}

func (url *URL) HasStream() bool {
	return url.Stream != nil
}

func (url *URL) HasStreamInChannel() bool {
	return url.HasChannel() && url.HasStream()
}

func (url *URL) GetParts() []*PathSegment {
	if url.HasStreamInChannel() {
		return []*PathSegment{url.Channel, url.Stream}
	}
	if url.HasChannel() {
		return []*PathSegment{url.Channel}
	}
	return []*PathSegment{url.Stream}
}

func (url *URL) String() string {
	parts := url.GetParts()
	stringParts := make([]string, len(parts))
	for i, x := range parts {
		stringParts[i] = x.String()
	}
	return "lbry://" + strings.Join(stringParts, "/")
}

func ParseURL(url string) *URL {
	segmentNames := []string{"channel", "stream", "channel_with_stream", "stream_in_channel"}
	re := createUrlRegex()

	match := re.FindStringSubmatch(url)
	parts := make(map[string]string)
	for i, name := range re.SubexpNames() {
		if i != 0 && name != "" {
			parts[name] = match[i]
		}
	}

	segments := make(map[string]*PathSegment)
	var amountOrder int
	for _, segment := range segmentNames {
		if res, ok := parts[segment + "_name"]; ok && res != ""{
			x, ok := parts[segment + "_amount_order"]
			if ok && x != "" {
				parsedInt, err := strconv.Atoi(x)
				if err != nil {
					log.Fatalln("can't parse amount_order")
				}
				amountOrder = parsedInt
			} else {
				amountOrder = -1
			}
			segments[segment] = &PathSegment{
				Name: parts[segment + "_name"],
				ClaimId: parts[segment + "_claim_id"],
				AmountOrder: amountOrder,
			}
		}
	}

	var stream *PathSegment = nil
	var channel *PathSegment = nil
	if _, ok := segments["channel_with_stream"]; ok {
		stream = segments["channel_with_stream"]
		channel = segments["stream_in_channel"]
	} else {
		stream = segments["channel"]
		channel = segments["stream"]
	}

	return &URL{stream,channel}
}

func createUrlRegex() *regexp.Regexp {
	d800 := []uint16{0xd800}
	dfff := []uint16{0xdfff}
	s1 := string(utf16.Decode(d800))
	s2 := string(utf16.Decode(dfff))
	log.Println(s1)
	log.Println(s2)
	//invalidNamesRegex := "[^=&#:$@%?;\"/\\<>%{}|^~`\\[\\]" + "\\u0000-\\u0020\\uD800-\\uDFFF\\uFFFE-\\uFFFF]+"
	invalidNamesRegex := "[^=&#:$@%?;\"/\\<>%{}|^~`\\[\\]" + "\u0000-\u0020" + s1 + "-" + s2 + "\uFFFE-\uFFFF]+"
	//invalidNamesRegex := "[^=&#:$@%?;\"/\\<>%{}|^~`\\[\\]" + "\u0000-\u0020-\uFFFE-\uFFFF]+"
	//invalidNamesRegex := "[^=&#:$@%?;\"/\\<>%{}|^~`\\[\\]" + "]+"

	named := func (name string, regex string) string {
		return "(?P<" + name + ">" + regex + ")"
	}

	group := func(regex string) string {
		return "(?:" + regex + ")"
	}

	oneof := func(choices []string) string {
		return group(strings.Join(choices, "|"))
	}

	claim := func(name string, prefix string) string {
		return group(
			named(name+"_name", prefix + invalidNamesRegex) +
			oneof(
				[]string {
					group("[:#]" + named(name+"_claim_id", "[0-9a-f]{1,40}")),
					group("\\$" + named(name+"_amount_order", "[1-9][0-9]*")),
				},
			) + "?",
		)
	}

	finalStr := "^" +
		named("scheme", "lbry://") + "?" +
		oneof(
			[]string {
				group(claim("channel_with_stream", "@") + "/" + claim("stream_in_channel", "")),
				claim("channel", "@"),
				claim("stream", ""),
			},
		) +
		"$"

	re := regexp.MustCompile(finalStr)
	return re
}