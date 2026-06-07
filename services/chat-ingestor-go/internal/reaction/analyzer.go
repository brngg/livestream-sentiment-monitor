package reaction

import (
	"math"
	"sort"
	"strings"
	"time"
	"unicode"

	"stream-reaction-intelligence/services/chat-ingestor-go/internal/chat"
)

const (
	DefaultWindow    = 5 * time.Second
	DefaultRetention = 5 * time.Minute
)

type Analyzer struct {
	window        time.Duration
	retention     time.Duration
	messages      []chat.ChatMessage
	recentWindows []chat.ReactionWindow
}

func NewAnalyzer(window, retention time.Duration) *Analyzer {
	if window <= 0 {
		window = DefaultWindow
	}
	if retention <= 0 {
		retention = DefaultRetention
	}
	return &Analyzer{window: window, retention: retention}
}

func (a *Analyzer) Add(message chat.ChatMessage) {
	message.Timestamp = message.Timestamp.UTC()
	a.messages = append(a.messages, message)
	a.prune(message.Timestamp.Add(-a.retention - a.window))
}

func (a *Analyzer) WindowAt(end time.Time, sessionID, channelID string) chat.ReactionWindow {
	end = end.UTC()
	start := end.Add(-a.window)
	a.prune(end.Add(-a.retention - a.window))

	current := a.messagesIn(start, end)
	previous := a.messagesIn(start.Add(-a.window), start)
	window := a.score(current, previous, start, end, sessionID, channelID)
	a.recentWindows = append([]chat.ReactionWindow{window}, a.recentWindows...)
	a.recentWindows = retainWindows(a.recentWindows, end.Add(-a.retention))
	return window
}

func (a *Analyzer) RecentWindows() []chat.ReactionWindow {
	out := make([]chat.ReactionWindow, len(a.recentWindows))
	copy(out, a.recentWindows)
	return out
}

func (a *Analyzer) messagesIn(start, end time.Time) []chat.ChatMessage {
	out := make([]chat.ChatMessage, 0)
	for _, message := range a.messages {
		if !message.Timestamp.Before(start) && message.Timestamp.Before(end) {
			out = append(out, message)
		}
	}
	return out
}

func (a *Analyzer) prune(cutoff time.Time) {
	keepFrom := 0
	for keepFrom < len(a.messages) && a.messages[keepFrom].Timestamp.Before(cutoff) {
		keepFrom++
	}
	if keepFrom > 0 {
		a.messages = append([]chat.ChatMessage(nil), a.messages[keepFrom:]...)
	}
}

func (a *Analyzer) score(messages, previous []chat.ChatMessage, start, end time.Time, sessionID, channelID string) chat.ReactionWindow {
	chatters := map[string]struct{}{}
	repeats := map[string]int{}
	var hypeHits, confusionHits, frustrationHits int
	var capsRatioTotal, punctuationTotal, shortTotal, valenceTotal float64
	var valenceHits int

	for _, message := range messages {
		name := strings.TrimSpace(message.Username)
		if name == "" {
			name = message.DisplayName
		}
		if name != "" {
			chatters[strings.ToLower(name)] = struct{}{}
		}

		normalized := normalizedText(message.Text)
		if normalized != "" {
			repeats[normalized]++
		}
		tokens := tokenSet(message.Text)
		hypeHits += countMatches(tokens, hypeTerms)
		hypeHits += countPhraseMatches(message.Text, hypePhrases)
		confusionHits += countMatches(tokens, confusionTerms)
		frustrationHits += countMatches(tokens, frustrationTerms)
		if strings.Contains(strings.ToUpper(message.Text), "???") {
			confusionHits += 2
		}
		if strings.Contains(message.Text, "?") {
			confusionHits++
		}
		if len(message.Emotes) > 0 {
			hypeHits += len(message.Emotes)
		}
		if isShortBurstMessage(message.Text) {
			shortTotal++
		}
		capsRatioTotal += uppercaseRatio(message.Text)
		punctuationTotal += punctuationBurstScore(message.Text)
		if score, ok := valenceScore(tokens); ok {
			valenceTotal += score
			valenceHits++
		}
	}

	messageCount := len(messages)
	windowSeconds := math.Max(a.window.Seconds(), 1)
	messagesPerMinute := float64(messageCount) / windowSeconds * 60
	previousRate := float64(len(previous)) / windowSeconds * 60
	velocityScore := clamp01(messagesPerMinute / 120)
	if previousRate > 0 {
		velocityScore = clamp01(math.Max(velocityScore, (messagesPerMinute-previousRate)/math.Max(previousRate, 1)))
	}

	repeatRatio := repeatRatio(repeats, messageCount)
	uniqueSpike := uniqueSpikeScore(messages, previous)
	capsAverage := average(capsRatioTotal, messageCount)
	punctuationAverage := average(punctuationTotal, messageCount)
	shortRatio := average(shortTotal, messageCount)

	hypeScore := clamp01(float64(hypeHits)/math.Max(float64(messageCount), 1)*1.8 + shortRatio*0.2 + repeatRatio*0.25 + capsAverage*0.3)
	confusionScore := clamp01(float64(confusionHits) / math.Max(float64(messageCount), 1) * 1.5)
	frustrationScore := clamp01(float64(frustrationHits)/math.Max(float64(messageCount), 1)*1.5 + negativeValenceBoost(valenceTotal, valenceHits))
	intensityScore := clamp01(velocityScore*0.35 + capsAverage*0.2 + punctuationAverage*0.18 + repeatRatio*0.15 + uniqueSpike*0.12)
	valence := average(valenceTotal, valenceHits)
	reaction := reactionType(hypeScore, confusionScore, frustrationScore, intensityScore)
	confidence := reactionConfidence(reaction, hypeScore, confusionScore, frustrationScore, intensityScore)
	evidence := evidenceMessages(messages)
	evidenceIDs := evidenceMessageIDs(evidence)
	targetText := targetText(messages)
	if reaction == "neutral" || confidence < 0.35 {
		targetText = ""
	}

	return chat.ReactionWindow{
		Type:              "reaction_window",
		SessionID:         sessionID,
		ChannelID:         channelID,
		Source:            "chat",
		WindowStart:       start,
		WindowEnd:         end,
		MessageCount:      messageCount,
		UniqueChatters:    len(chatters),
		MessagesPerMinute: roundFloat(messagesPerMinute),
		VelocityScore:     roundFloat(velocityScore),
		HypeScore:         roundFloat(hypeScore),
		IntensityScore:    roundFloat(intensityScore),
		ConfusionScore:    roundFloat(confusionScore),
		FrustrationScore:  roundFloat(frustrationScore),
		Valence:           roundFloat(clamp(valence, -1, 1)),
		ReactionType:      reaction,
		TargetType:        "unknown",
		TargetText:        targetText,
		EventHint:         eventHint(reaction, targetText),
		Confidence:        roundFloat(confidence),
		EvidenceIDs:       evidenceIDs,
		EvidenceMessages:  evidence,
	}
}

func reactionConfidence(reaction string, hype, confusion, frustration, intensity float64) float64 {
	switch reaction {
	case "hype":
		return clamp01(hype)
	case "confusion":
		return clamp01(confusion)
	case "frustration":
		return clamp01(frustration)
	case "intensity":
		return clamp01(intensity)
	default:
		return clamp01(intensity * 0.5)
	}
}

func eventHint(reactionType, targetText string) string {
	if targetText == "" {
		return reactionType
	}
	return reactionType + ":" + targetText
}

func evidenceMessageIDs(messages []chat.ChatMessage) []string {
	out := make([]string, 0, len(messages))
	seen := map[string]struct{}{}
	for _, message := range messages {
		if message.MessageID == "" {
			continue
		}
		if _, ok := seen[message.MessageID]; ok {
			continue
		}
		seen[message.MessageID] = struct{}{}
		out = append(out, message.MessageID)
	}
	return out
}

func targetText(messages []chat.ChatMessage) string {
	counts := map[string]int{}
	firstSeen := map[string]int{}
	for index, message := range messages {
		for token := range tokenSet(message.Text) {
			if _, skip := targetStopWords[token]; skip || len(token) < 3 {
				continue
			}
			counts[token]++
			if _, ok := firstSeen[token]; !ok {
				firstSeen[token] = index
			}
		}
	}
	type candidate struct {
		text  string
		count int
		index int
	}
	var candidates []candidate
	for text, count := range counts {
		if count < 2 {
			continue
		}
		if float64(count)/math.Max(float64(len(messages)), 1) < 0.4 {
			continue
		}
		candidates = append(candidates, candidate{text: text, count: count, index: firstSeen[text]})
	}
	sort.Slice(candidates, func(i, j int) bool {
		if candidates[i].count == candidates[j].count {
			return candidates[i].index < candidates[j].index
		}
		return candidates[i].count > candidates[j].count
	})
	if len(candidates) == 0 {
		return ""
	}
	return candidates[0].text
}

func evidenceMessages(messages []chat.ChatMessage) []chat.ChatMessage {
	items := make([]chat.ChatMessage, len(messages))
	copy(items, messages)
	sort.SliceStable(items, func(i, j int) bool {
		return evidenceScore(items[i]) > evidenceScore(items[j])
	})
	if len(items) > 5 {
		items = items[:5]
	}
	return items
}

func evidenceScore(message chat.ChatMessage) float64 {
	tokens := tokenSet(message.Text)
	return float64(countMatches(tokens, hypeTerms))*1.2 +
		float64(countMatches(tokens, confusionTerms))*1.1 +
		float64(countMatches(tokens, frustrationTerms))*1.1 +
		uppercaseRatio(message.Text)*0.5 +
		punctuationBurstScore(message.Text)*0.3 +
		float64(len(message.Emotes))*0.4
}

func retainWindows(windows []chat.ReactionWindow, cutoff time.Time) []chat.ReactionWindow {
	out := windows[:0]
	for _, window := range windows {
		if !window.WindowEnd.Before(cutoff) {
			out = append(out, window)
		}
	}
	return out
}

func reactionType(hype, confusion, frustration, intensity float64) string {
	type candidate struct {
		label string
		score float64
	}
	candidates := []candidate{
		{label: "hype", score: hype},
		{label: "confusion", score: confusion},
		{label: "frustration", score: frustration},
		{label: "intensity", score: intensity},
	}
	sort.Slice(candidates, func(i, j int) bool {
		return candidates[i].score > candidates[j].score
	})
	if candidates[0].score < 0.2 {
		return "neutral"
	}
	return candidates[0].label
}

func tokenSet(text string) map[string]struct{} {
	out := map[string]struct{}{}
	for _, token := range strings.FieldsFunc(strings.ToLower(text), func(r rune) bool {
		return !(r >= 'a' && r <= 'z') && !(r >= '0' && r <= '9')
	}) {
		if token != "" {
			out[token] = struct{}{}
		}
	}
	return out
}

func countMatches(tokens map[string]struct{}, terms map[string]struct{}) int {
	var count int
	for term := range terms {
		if _, ok := tokens[term]; ok {
			count++
		}
	}
	return count
}

func countPhraseMatches(text string, phrases []string) int {
	normalized := " " + normalizedText(text) + " "
	var count int
	for _, phrase := range phrases {
		if strings.Contains(normalized, " "+phrase+" ") {
			count++
		}
	}
	return count
}

func normalizedText(text string) string {
	return strings.Join(strings.Fields(strings.ToLower(text)), " ")
}

func isShortBurstMessage(text string) bool {
	trimmed := strings.TrimSpace(text)
	return len([]rune(trimmed)) > 0 && len([]rune(trimmed)) <= 12
}

func uppercaseRatio(text string) float64 {
	var letters, upper int
	for _, r := range text {
		if unicode.IsLetter(r) {
			letters++
			if unicode.IsUpper(r) {
				upper++
			}
		}
	}
	if letters < 3 {
		return 0
	}
	return float64(upper) / float64(letters)
}

func punctuationBurstScore(text string) float64 {
	var count int
	for _, r := range text {
		if r == '!' || r == '?' {
			count++
		}
	}
	return clamp01(float64(count) / 4)
}

func repeatRatio(repeats map[string]int, total int) float64 {
	if total <= 1 {
		return 0
	}
	var repeated int
	for _, count := range repeats {
		if count > 1 {
			repeated += count
		}
	}
	return float64(repeated) / float64(total)
}

func uniqueSpikeScore(messages, previous []chat.ChatMessage) float64 {
	current := uniqueChatterCount(messages)
	prev := uniqueChatterCount(previous)
	if current == 0 {
		return 0
	}
	if prev == 0 {
		return clamp01(float64(current) / 10)
	}
	return clamp01(float64(current-prev) / math.Max(float64(prev), 1))
}

func uniqueChatterCount(messages []chat.ChatMessage) int {
	chatters := map[string]struct{}{}
	for _, message := range messages {
		name := strings.ToLower(strings.TrimSpace(message.Username))
		if name == "" {
			name = strings.ToLower(strings.TrimSpace(message.DisplayName))
		}
		if name != "" {
			chatters[name] = struct{}{}
		}
	}
	return len(chatters)
}

func valenceScore(tokens map[string]struct{}) (float64, bool) {
	var total float64
	var hits int
	for term, score := range valenceTerms {
		if _, ok := tokens[term]; ok {
			total += score
			hits++
		}
	}
	if hits == 0 {
		return 0, false
	}
	return total / float64(hits), true
}

func negativeValenceBoost(total float64, hits int) float64 {
	if hits == 0 || total >= 0 {
		return 0
	}
	return clamp01(math.Abs(total/float64(hits)) * 0.25)
}

func average(total float64, count int) float64 {
	if count == 0 {
		return 0
	}
	return total / float64(count)
}

func roundFloat(value float64) float64 {
	return math.Round(value*1000) / 1000
}

func clamp01(value float64) float64 {
	return clamp(value, 0, 1)
}

func clamp(value, minValue, maxValue float64) float64 {
	if value < minValue {
		return minValue
	}
	if value > maxValue {
		return maxValue
	}
	return value
}

var hypeTerms = map[string]struct{}{
	"w": {}, "wow": {}, "holy": {}, "insane": {}, "clutch": {}, "lfg": {},
	"pog": {}, "pogchamp": {}, "poggers": {}, "hype": {}, "crazy": {},
}

var hypePhrases = []string{"no way"}

var confusionTerms = map[string]struct{}{
	"huh": {}, "what": {}, "wait": {}, "why": {}, "confused": {}, "lost": {},
}

var frustrationTerms = map[string]struct{}{
	"l": {}, "throw": {}, "threw": {}, "choke": {}, "choked": {}, "bad": {},
	"troll": {}, "awful": {}, "terrible": {}, "hate": {}, "scam": {},
}

var valenceTerms = map[string]float64{
	"amazing": 1, "clutch": 0.8, "funny": 0.3, "gg": 0.7, "great": 0.8,
	"hype": 0.6, "insane": 0.4, "love": 1, "pog": 0.7, "pogchamp": 0.8,
	"awful": -1, "bad": -0.7, "choke": -0.8, "hate": -1, "rough": -0.5,
	"sad": -0.5, "terrible": -1, "throw": -0.8, "troll": -0.7,
}

var targetStopWords = map[string]struct{}{
	"the": {}, "and": {}, "for": {}, "you": {}, "that": {}, "this": {}, "with": {}, "was": {}, "are": {}, "but": {},
	"what": {}, "wait": {}, "huh": {}, "why": {}, "how": {}, "wow": {}, "holy": {}, "insane": {}, "clutch": {}, "bad": {},
	"throw": {}, "choke": {}, "troll": {}, "lfg": {}, "pog": {}, "way": {}, "not": {}, "now": {}, "just": {},
}
