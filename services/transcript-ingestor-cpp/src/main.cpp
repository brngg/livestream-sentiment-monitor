#include <arpa/inet.h>
#include <algorithm>
#include <array>
#include <atomic>
#include <csignal>
#include <cerrno>
#include <chrono>
#include <condition_variable>
#include <cctype>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <deque>
#include <fcntl.h>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <map>
#include <memory>
#include <mutex>
#include <netinet/in.h>
#include <optional>
#include <poll.h>
#include <queue>
#include <regex>
#include <set>
#include <sstream>
#include <string>
#include <sys/socket.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <thread>
#include <unistd.h>
#include <vector>

namespace fs = std::filesystem;
using Clock = std::chrono::system_clock;

const std::string kNemotronAsrStreamingFunctionId = "bb0837de-8c7b-481f-9ec8-ef5663e9c1fa";
const std::string kParakeetMultilingualAsrFunctionId = "71203149-d3b7-4460-8231-1be2543a1fca";

struct Config {
    std::string host = "0.0.0.0";
    int port = 8092;
    int default_chunk_seconds = 1;
    std::string asr_provider = "nvidia_streaming";
    int asr_queue_max_depth = 4;
    bool repair_enabled = false;
    int repair_queue_max_depth = 2;
    std::string nvidia_python = "python3";
    std::string nvidia_helper = "scripts/nvidia_asr_transcribe.py";
    std::string nvidia_streaming_helper = "scripts/nvidia_live_stream_asr.py";
    std::string nvidia_server = "grpc.nvcf.nvidia.com:443";
    std::string nvidia_function_id = kNemotronAsrStreamingFunctionId;
    std::string nvidia_model_name;
    std::string nvidia_language_code = "en-US";
    int nvidia_file_streaming_chunk = 1600;
    std::string analyzer_url = "http://sentiment-analyzer:8091";
    int analyzer_timeout_seconds = 15;
};

struct Segment {
    double start = 0;
    double end = 0;
    std::string text;
    double confidence = 0.80;
};

struct Bucket {
    std::string type = "transcript_bucket";
    std::string session_id;
    std::string channel_id;
    Clock::time_point bucket_start;
    Clock::time_point bucket_end;
    Clock::time_point audio_started_at;
    Clock::time_point audio_ended_at;
    Clock::time_point transcribed_at;
    std::string text;
    std::string language = "en";
    double transcript_confidence = 0.80;
    std::optional<double> sentiment_score;
    std::optional<double> sentiment_confidence;
    std::string sentiment_label;
    std::string sentiment_model;
    std::string sentiment_status = "skipped";
    std::optional<long long> sentiment_latency_ms;
    std::optional<long long> asr_latency_ms;
    std::optional<long long> pipeline_latency_ms;
    bool repaired = false;
    std::string repair_status;
    std::optional<long long> repair_latency_ms;
    std::string original_live_text;
    std::vector<Segment> segments;
    std::vector<fs::path> retained_audio_chunks;
};

struct TranscriptUpdate {
    std::string type = "transcript_segment";
    std::string session_id;
    std::string channel_id;
    Clock::time_point transcript_start;
    Clock::time_point transcript_end;
    Clock::time_point audio_started_at;
    Clock::time_point audio_ended_at;
    Clock::time_point transcribed_at;
    std::string text;
    std::string language = "en";
    double transcript_confidence = 0.80;
    std::optional<long long> asr_latency_ms;
    std::optional<long long> pipeline_latency_ms;
    std::vector<Segment> segments;
};

struct Session {
    std::string session_id;
    std::string channel_id;
    std::string twitch_url;
    int bucket_seconds = 30;
    int chunk_seconds = 1;
    std::string status = "idle";
    std::string error;
    int partial_count = 0;
    int bucket_count = 0;
    int segment_count = 0;
    std::vector<Bucket> buckets;
    std::vector<TranscriptUpdate> segments;
    std::optional<Bucket> latest_bucket;
    std::optional<TranscriptUpdate> latest_segment;
    Clock::time_point created_at = Clock::now();
};

struct Subscriber {
    std::mutex mutex;
    std::condition_variable cv;
    std::queue<std::string> events;
    bool closed = false;
};

std::mutex g_mutex;
std::vector<std::weak_ptr<Subscriber>> g_subscribers;
std::optional<Session> g_session;
std::shared_ptr<std::atomic_bool> g_stop;
std::thread g_worker;
Config g_config;

std::string env_string(const char *key, const std::string &fallback) {
    const char *value = std::getenv(key);
    if (value == nullptr || std::string(value).empty()) return fallback;
    return value;
}

int env_int(const char *key, int fallback) {
    const char *value = std::getenv(key);
    if (value == nullptr || std::string(value).empty()) return fallback;
    try {
        return std::stoi(value);
    } catch (...) {
        return fallback;
    }
}

bool env_bool(const char *key, bool fallback) {
    const char *raw = std::getenv(key);
    if (raw == nullptr || std::string(raw).empty()) return fallback;
    std::string value = raw;
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) { return std::tolower(c); });
    if (value == "1" || value == "true" || value == "yes" || value == "on" || value == "enabled") return true;
    if (value == "0" || value == "false" || value == "no" || value == "off" || value == "disabled") return false;
    return fallback;
}

bool env_present(const char *key) {
    const char *value = std::getenv(key);
    return value != nullptr && !std::string(value).empty();
}

std::string first_env_string(const std::vector<const char *> &keys, const std::string &fallback) {
    for (const char *key : keys) {
        const char *value = std::getenv(key);
        if (value != nullptr && !std::string(value).empty()) return value;
    }
    return fallback;
}

std::string normalized_asr_provider() {
    std::string provider = g_config.asr_provider;
    std::transform(provider.begin(), provider.end(), provider.begin(), [](unsigned char c) {
        if (c == '-') return '_';
        return static_cast<char>(std::tolower(c));
    });
    if (provider.empty()) return "nvidia_streaming";
    if (provider == "nvidia" || provider == "nvidia_nim" || provider == "nvidia_asr") return "nvidia_streaming";
    if (provider == "nvidia_live" || provider == "nvidia_nim_live" ||
        provider == "nvidia_stream" || provider == "nvidia_nim_stream" ||
        provider == "nvidia_streaming" || provider == "nvidia_nim_streaming") {
        return "nvidia_streaming";
    }
    if (provider == "nvidia_hosted" || provider == "nvidia_chunked" || provider == "nvidia_file") return "nvidia_hosted";
    return provider;
}

bool using_nvidia_hosted_asr() {
    return normalized_asr_provider() == "nvidia_hosted";
}

bool using_nvidia_streaming_asr() {
    return normalized_asr_provider() == "nvidia_streaming";
}

bool using_nvidia_asr() {
    return using_nvidia_hosted_asr() || using_nvidia_streaming_asr();
}

std::string asr_source_name() {
    if (using_nvidia_streaming_asr()) return "nvidia-nim-streaming";
    if (using_nvidia_hosted_asr()) return "nvidia-nim-hosted";
    return "unsupported-asr-provider";
}

std::string transcript_language_code() {
    return g_config.nvidia_language_code.empty() ? "unknown" : g_config.nvidia_language_code;
}

std::string nvidia_asr_model_display_name() {
    if (!g_config.nvidia_model_name.empty()) return g_config.nvidia_model_name;
    if (g_config.nvidia_function_id == kParakeetMultilingualAsrFunctionId) return "parakeet-1.1b-rnnt-multilingual-asr";
    if (g_config.nvidia_function_id == kNemotronAsrStreamingFunctionId) return "nemotron-asr-streaming";
    return "nvidia-hosted-asr";
}

std::string trim(const std::string &value) {
    size_t start = 0;
    while (start < value.size() && std::isspace(static_cast<unsigned char>(value[start]))) start++;
    size_t end = value.size();
    while (end > start && std::isspace(static_cast<unsigned char>(value[end - 1]))) end--;
    return value.substr(start, end - start);
}

std::string json_escape(const std::string &value) {
    std::ostringstream out;
    for (char ch : value) {
        switch (ch) {
            case '"': out << "\\\""; break;
            case '\\': out << "\\\\"; break;
            case '\b': out << "\\b"; break;
            case '\f': out << "\\f"; break;
            case '\n': out << "\\n"; break;
            case '\r': out << "\\r"; break;
            case '\t': out << "\\t"; break;
            default:
                if (static_cast<unsigned char>(ch) < 0x20) {
                    out << "\\u" << std::hex << std::setw(4) << std::setfill('0') << static_cast<int>(ch);
                } else {
                    out << ch;
                }
        }
    }
    return out.str();
}

std::string json_string(const std::string &value) {
    return "\"" + json_escape(value) + "\"";
}

std::string iso_time(Clock::time_point value) {
    auto t = Clock::to_time_t(value);
    auto micros = std::chrono::duration_cast<std::chrono::microseconds>(value.time_since_epoch()).count() % 1000000;
    std::tm tm{};
    gmtime_r(&t, &tm);
    std::ostringstream out;
    out << std::put_time(&tm, "%Y-%m-%dT%H:%M:%S");
    if (micros > 0) out << "." << std::setw(6) << std::setfill('0') << micros;
    out << "Z";
    return out.str();
}

long long ms_between(Clock::time_point start, Clock::time_point end) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count();
}

std::string shell_quote(const std::string &value) {
    std::string result = "'";
    for (char ch : value) {
        if (ch == '\'') result += "'\\''";
        else result += ch;
    }
    result += "'";
    return result;
}

std::string json_unescape(const std::string &value) {
    std::string result;
    result.reserve(value.size());
    for (size_t i = 0; i < value.size(); i++) {
        char ch = value[i];
        if (ch != '\\' || i + 1 >= value.size()) {
            result += ch;
            continue;
        }
        char next = value[++i];
        switch (next) {
            case '"': result += '"'; break;
            case '\\': result += '\\'; break;
            case '/': result += '/'; break;
            case 'b': result += '\b'; break;
            case 'f': result += '\f'; break;
            case 'n': result += '\n'; break;
            case 'r': result += '\r'; break;
            case 't': result += '\t'; break;
            default:
                result += next;
                break;
        }
    }
    return result;
}

std::optional<std::string> json_field(const std::string &body, const std::string &key) {
    std::regex pattern("\"" + key + "\"\\s*:\\s*\"((?:\\\\.|[^\"])*)\"");
    std::smatch match;
    if (std::regex_search(body, match, pattern)) return json_unescape(match[1].str());
    return std::nullopt;
}

std::optional<int> json_int_field(const std::string &body, const std::string &key) {
    std::regex pattern("\"" + key + "\"\\s*:\\s*([0-9]+)");
    std::smatch match;
    if (std::regex_search(body, match, pattern)) return std::stoi(match[1].str());
    return std::nullopt;
}

std::string extract_channel(const std::string &channel_or_url) {
    std::string value = trim(channel_or_url);
    std::string lower = value;
    std::transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char c) { return std::tolower(c); });
    if ((lower.find("youtube.com/") != std::string::npos || lower.find("youtu.be/") != std::string::npos)) {
        std::string id;
        auto vpos = lower.find("v=");
        if (vpos != std::string::npos) {
            id = value.substr(vpos + 2);
            auto end = id.find_first_of("&#");
            if (end != std::string::npos) id = id.substr(0, end);
        } else {
            for (const std::string marker : {"/live/", "/embed/", "/shorts/", "youtu.be/"}) {
                auto pos = lower.find(marker);
                if (pos == std::string::npos) continue;
                id = value.substr(pos + marker.size());
                auto end = id.find_first_of("/?&#");
                if (end != std::string::npos) id = id.substr(0, end);
                break;
            }
        }
        if (id.empty()) {
            auto scheme = lower.find("://");
            auto start = scheme == std::string::npos ? 0 : scheme + 3;
            auto slash = lower.find('/', start);
            if (slash != std::string::npos) {
                id = value.substr(slash + 1);
                auto end = id.find_first_of("?&#");
                if (end != std::string::npos) id = id.substr(0, end);
                std::replace(id.begin(), id.end(), '/', '-');
            }
        }
        std::string sanitized;
        for (unsigned char c : id) {
            if (std::isalnum(c) || c == '_' || c == '-') sanitized.push_back(static_cast<char>(std::tolower(c)));
            else if (!sanitized.empty() && sanitized.back() != '-') sanitized.push_back('-');
        }
        while (!sanitized.empty() && sanitized.back() == '-') sanitized.pop_back();
        return sanitized.empty() ? "" : "youtube-" + sanitized;
    }
    if (value.find("://") != std::string::npos && value.find("twitch.tv/") == std::string::npos) return "";
    auto pos = value.find("twitch.tv/");
    if (pos != std::string::npos) {
        value = value.substr(pos + std::string("twitch.tv/").size());
        auto slash = value.find('/');
        if (slash != std::string::npos) value = value.substr(0, slash);
        auto query = value.find('?');
        if (query != std::string::npos) value = value.substr(0, query);
    }
    if (value.rfind("@", 0) == 0) value = value.substr(1);
    if (value.rfind("#", 0) == 0) value = value.substr(1);
    std::transform(value.begin(), value.end(), value.begin(), [](unsigned char c) { return std::tolower(c); });
    if (value.empty() || value.find('/') != std::string::npos || value.find(' ') != std::string::npos) return "";
    return value;
}

std::string transcript_url(const std::string &channel_or_url) {
    std::string value = trim(channel_or_url);
    std::string lower = value;
    std::transform(lower.begin(), lower.end(), lower.begin(), [](unsigned char c) { return std::tolower(c); });
    if (lower.find("twitch.tv") != std::string::npos || lower.find("youtube.com") != std::string::npos || lower.find("youtu.be") != std::string::npos) return value;
    std::string channel = extract_channel(value);
    return channel.empty() ? "" : "https://www.twitch.tv/" + channel;
}

std::string run_capture(const std::string &command) {
    std::array<char, 4096> buffer{};
    std::string result;
    FILE *pipe = popen(command.c_str(), "r");
    if (!pipe) return result;
    while (fgets(buffer.data(), static_cast<int>(buffer.size()), pipe) != nullptr) {
        result += buffer.data();
    }
    pclose(pipe);
    return result;
}

pid_t start_process_group(const std::string &command) {
    pid_t pid = fork();
    if (pid == 0) {
        setsid();
        execl("/bin/sh", "sh", "-c", command.c_str(), static_cast<char *>(nullptr));
        _exit(127);
    }
    return pid;
}

pid_t start_process_group_capture(const std::string &command, int *read_fd) {
    int fds[2] = {-1, -1};
    if (pipe(fds) != 0) return -1;
    pid_t pid = fork();
    if (pid == 0) {
        setsid();
        close(fds[0]);
        dup2(fds[1], STDOUT_FILENO);
        int dev_null = open("/dev/null", O_WRONLY);
        if (dev_null >= 0) {
            dup2(dev_null, STDERR_FILENO);
            close(dev_null);
        }
        close(fds[1]);
        execl("/bin/sh", "sh", "-c", command.c_str(), static_cast<char *>(nullptr));
        _exit(127);
    }
    close(fds[1]);
    if (pid < 0) {
        close(fds[0]);
        return -1;
    }
    int flags = fcntl(fds[0], F_GETFL, 0);
    if (flags >= 0) fcntl(fds[0], F_SETFL, flags | O_NONBLOCK);
    *read_fd = fds[0];
    return pid;
}

void stop_process_group(pid_t pid) {
    if (pid <= 0) return;
    kill(-pid, SIGTERM);
    for (int i = 0; i < 20; i++) {
        int status = 0;
        pid_t result = waitpid(pid, &status, WNOHANG);
        if (result == pid) return;
        std::this_thread::sleep_for(std::chrono::milliseconds(100));
    }
    kill(-pid, SIGKILL);
    int status = 0;
    waitpid(pid, &status, 0);
}

std::string segment_json(const Segment &segment) {
    std::ostringstream out;
    out << "{\"start\":" << segment.start
        << ",\"end\":" << segment.end
        << ",\"text\":" << json_string(segment.text)
        << ",\"confidence\":" << segment.confidence
        << ",\"words\":[]}";
    return out.str();
}

std::string optional_number(std::optional<double> value) {
    if (!value) return "null";
    std::ostringstream out;
    out << *value;
    return out.str();
}

std::string optional_int(std::optional<long long> value) {
    if (!value) return "null";
    return std::to_string(*value);
}

double duration_seconds(Clock::time_point start, Clock::time_point end) {
    return std::chrono::duration_cast<std::chrono::milliseconds>(end - start).count() / 1000.0;
}

std::vector<std::string> transcript_words(const std::string &text) {
    std::vector<std::string> out;
    std::istringstream words(text);
    std::string word;
    while (words >> word) {
        std::string normalized;
        for (unsigned char ch : word) {
            if (std::isalnum(ch)) normalized.push_back(static_cast<char>(std::tolower(ch)));
        }
        if (!normalized.empty()) out.push_back(normalized);
    }
    return out;
}

int transcript_word_count(const std::string &text) {
    return static_cast<int>(transcript_words(text).size());
}

int word_edit_distance(const std::vector<std::string> &left, const std::vector<std::string> &right) {
    std::vector<int> previous(right.size() + 1);
    std::vector<int> current(right.size() + 1);
    for (size_t j = 0; j <= right.size(); j++) previous[j] = static_cast<int>(j);
    for (size_t i = 1; i <= left.size(); i++) {
        current[0] = static_cast<int>(i);
        for (size_t j = 1; j <= right.size(); j++) {
            int substitution = previous[j - 1] + (left[i - 1] == right[j - 1] ? 0 : 1);
            int deletion = previous[j] + 1;
            int insertion = current[j - 1] + 1;
            current[j] = std::min({substitution, deletion, insertion});
        }
        previous.swap(current);
    }
    return previous[right.size()];
}

double segment_coverage_seconds(const std::vector<Segment> &segments, double audio_seconds) {
    if (audio_seconds <= 0 || segments.empty()) return 0;
    std::vector<std::pair<double, double>> intervals;
    for (const auto &segment : segments) {
        double start = std::max(0.0, std::min(segment.start, audio_seconds));
        double end = std::max(0.0, std::min(segment.end, audio_seconds));
        if (end > start) intervals.push_back({start, end});
    }
    if (intervals.empty()) return 0;
    std::sort(intervals.begin(), intervals.end());
    double covered = 0;
    double current_start = intervals[0].first;
    double current_end = intervals[0].second;
    for (size_t i = 1; i < intervals.size(); i++) {
        if (intervals[i].first <= current_end) {
            current_end = std::max(current_end, intervals[i].second);
            continue;
        }
        covered += current_end - current_start;
        current_start = intervals[i].first;
        current_end = intervals[i].second;
    }
    covered += current_end - current_start;
    return std::min(covered, audio_seconds);
}

std::string transcript_bucket_status(const Bucket &bucket) {
    std::string repair_status = bucket.repair_status;
    std::transform(repair_status.begin(), repair_status.end(), repair_status.begin(), [](unsigned char ch) {
        return static_cast<char>(std::tolower(ch));
    });
    if (repair_status == "pending" || repair_status == "queued") return "repairing";
    if (repair_status == "completed") return "final";
    if (repair_status == "failed" || repair_status == "queue_full" || repair_status == "audio_read_failed" ||
        repair_status == "audio_write_failed" || repair_status == "empty") {
        return "degraded";
    }
    if (repair_status == "disabled" || repair_status == "no_audio") return bucket.text.empty() ? "degraded" : "final";
    return bucket.repaired ? "final" : "live";
}

struct BucketCompleteness {
    double audio_seconds = 0;
    int segment_count = 0;
    int word_count = 0;
    double empty_ratio = 0;
    int repair_added_words = 0;
    double repair_changed_ratio = 0;
    std::string transcript_status = "live";
};

BucketCompleteness bucket_completeness(const Bucket &bucket) {
    BucketCompleteness metrics;
    metrics.audio_seconds = std::max(0.0, duration_seconds(bucket.audio_started_at, bucket.audio_ended_at));
    metrics.segment_count = static_cast<int>(bucket.segments.size());
    metrics.word_count = transcript_word_count(bucket.text);
    double covered = segment_coverage_seconds(bucket.segments, metrics.audio_seconds);
    metrics.empty_ratio = metrics.audio_seconds > 0 ? std::max(0.0, std::min(1.0, 1.0 - covered / metrics.audio_seconds)) : (bucket.text.empty() ? 1.0 : 0.0);
    metrics.transcript_status = transcript_bucket_status(bucket);
    if (!bucket.original_live_text.empty() || bucket.repaired) {
        auto live_words = transcript_words(bucket.original_live_text);
        auto final_words = transcript_words(bucket.text);
        metrics.repair_added_words = std::max(0, static_cast<int>(final_words.size()) - static_cast<int>(live_words.size()));
        size_t denominator = std::max(live_words.size(), final_words.size());
        metrics.repair_changed_ratio = denominator > 0 ? static_cast<double>(word_edit_distance(live_words, final_words)) / static_cast<double>(denominator) : 0.0;
    }
    return metrics;
}

std::string bucket_json(const Bucket &bucket) {
    std::ostringstream segments;
    segments << "[";
    for (size_t i = 0; i < bucket.segments.size(); i++) {
        if (i > 0) segments << ",";
        segments << segment_json(bucket.segments[i]);
    }
    segments << "]";

    BucketCompleteness completeness = bucket_completeness(bucket);
    double bucket_duration_seconds = duration_seconds(bucket.bucket_start, bucket.bucket_end);

    std::ostringstream out;
    out << "{"
        << "\"type\":\"transcript_bucket\","
        << "\"session_id\":" << json_string(bucket.session_id) << ","
        << "\"channel_id\":" << json_string(bucket.channel_id) << ","
        << "\"bucket_start\":" << json_string(iso_time(bucket.bucket_start)) << ","
        << "\"bucket_end\":" << json_string(iso_time(bucket.bucket_end)) << ","
        << "\"audio_started_at\":" << json_string(iso_time(bucket.audio_started_at)) << ","
        << "\"audio_ended_at\":" << json_string(iso_time(bucket.audio_ended_at)) << ","
        << "\"transcribed_at\":" << json_string(iso_time(bucket.transcribed_at)) << ","
        << "\"asr_latency_ms\":" << optional_int(bucket.asr_latency_ms) << ","
        << "\"pipeline_latency_ms\":" << optional_int(bucket.pipeline_latency_ms) << ","
        << "\"text\":" << json_string(bucket.text) << ","
        << "\"language\":" << json_string(bucket.language) << ","
        << "\"transcript_confidence\":" << bucket.transcript_confidence << ","
        << "\"transcript_status\":" << json_string(completeness.transcript_status) << ","
        << "\"sentiment_score\":" << optional_number(bucket.sentiment_score) << ","
        << "\"sentiment_confidence\":" << optional_number(bucket.sentiment_confidence) << ","
        << "\"sentiment_label\":" << json_string(bucket.sentiment_label) << ","
        << "\"sentiment_model\":" << json_string(bucket.sentiment_model) << ","
        << "\"sentiment_status\":" << json_string(bucket.sentiment_status) << ","
        << "\"sentiment_latency_ms\":" << optional_int(bucket.sentiment_latency_ms) << ","
        << "\"audio_seconds\":" << completeness.audio_seconds << ","
        << "\"segment_count\":" << completeness.segment_count << ","
        << "\"word_count\":" << completeness.word_count << ","
        << "\"empty_ratio\":" << completeness.empty_ratio << ","
        << "\"repair_added_words\":" << completeness.repair_added_words << ","
        << "\"repair_changed_ratio\":" << completeness.repair_changed_ratio << ","
        << "\"segments\":" << segments.str() << ","
        << "\"quality\":{"
        << "\"source\":" << json_string(asr_source_name()) << ","
        << "\"status\":" << json_string(completeness.transcript_status) << ","
        << "\"final\":" << (completeness.transcript_status == "final" ? "true" : "false") << ","
        << "\"repaired\":" << (bucket.repaired ? "true" : "false") << ","
        << "\"repair_status\":" << json_string(bucket.repair_status) << ","
        << "\"repair_latency_ms\":" << optional_int(bucket.repair_latency_ms) << ","
        << "\"original_live_text\":" << json_string(bucket.original_live_text) << ","
        << "\"audio_coverage_seconds\":" << completeness.audio_seconds << ","
        << "\"audio_seconds\":" << completeness.audio_seconds << ","
        << "\"bucket_duration_seconds\":" << bucket_duration_seconds << ","
        << "\"char_count\":" << bucket.text.size() << ","
        << "\"word_count\":" << completeness.word_count << ","
        << "\"empty_ratio\":" << completeness.empty_ratio << ","
        << "\"repair_added_words\":" << completeness.repair_added_words << ","
        << "\"repair_changed_ratio\":" << completeness.repair_changed_ratio << ","
        << "\"raw_segment_count\":" << completeness.segment_count << ","
        << "\"retained_segment_count\":" << completeness.segment_count << ","
        << "\"dropped_low_confidence_count\":0,"
        << "\"dropped_repeat_count\":0,"
        << "\"retained_ratio\":1"
        << "}"
        << "}";
    return out.str();
}

std::string update_json(const TranscriptUpdate &update) {
    std::ostringstream segments;
    segments << "[";
    for (size_t i = 0; i < update.segments.size(); i++) {
        if (i > 0) segments << ",";
        segments << segment_json(update.segments[i]);
    }
    segments << "]";

    std::ostringstream out;
    out << "{"
        << "\"type\":" << json_string(update.type) << ","
        << "\"session_id\":" << json_string(update.session_id) << ","
        << "\"channel_id\":" << json_string(update.channel_id) << ","
        << "\"transcript_start\":" << json_string(iso_time(update.transcript_start)) << ","
        << "\"transcript_end\":" << json_string(iso_time(update.transcript_end)) << ","
        << "\"audio_started_at\":" << json_string(iso_time(update.audio_started_at)) << ","
        << "\"audio_ended_at\":" << json_string(iso_time(update.audio_ended_at)) << ","
        << "\"transcribed_at\":" << json_string(iso_time(update.transcribed_at)) << ","
        << "\"asr_latency_ms\":" << optional_int(update.asr_latency_ms) << ","
        << "\"pipeline_latency_ms\":" << optional_int(update.pipeline_latency_ms) << ","
        << "\"text\":" << json_string(update.text) << ","
        << "\"language\":" << json_string(update.language) << ","
        << "\"transcript_confidence\":" << update.transcript_confidence << ","
        << "\"segments\":" << segments.str() << ","
        << "\"quality\":{"
        << "\"source\":" << json_string(asr_source_name()) << ","
        << "\"final\":true,"
        << "\"stable\":true,"
        << "\"raw_segment_count\":" << update.segments.size() << ","
        << "\"retained_segment_count\":" << update.segments.size() << ","
        << "\"dropped_low_confidence_count\":0,"
        << "\"dropped_repeat_count\":0,"
        << "\"retained_ratio\":1"
        << "}"
        << "}";
    return out.str();
}

void broadcast(const std::string &event_json) {
    std::lock_guard<std::mutex> lock(g_mutex);
    std::vector<std::weak_ptr<Subscriber>> live;
    for (auto &weak : g_subscribers) {
        auto sub = weak.lock();
        if (!sub) continue;
        {
            std::lock_guard<std::mutex> sub_lock(sub->mutex);
            if (sub->events.size() > 200) sub->events.pop();
            sub->events.push(event_json);
        }
        sub->cv.notify_one();
        live.push_back(sub);
    }
    g_subscribers = live;
}

void set_status(const std::string &status, const std::string &error = "") {
    std::string session_id;
    {
        std::lock_guard<std::mutex> lock(g_mutex);
        if (!g_session) return;
        g_session->status = status;
        g_session->error = error;
        session_id = g_session->session_id;
    }
    std::ostringstream event;
    event << "{\"type\":\"status\",\"session_id\":" << json_string(session_id)
          << ",\"status\":" << json_string(status);
    if (!error.empty()) event << ",\"error\":" << json_string(error);
    event << "}";
    broadcast(event.str());
}

void emit_asr_backpressure(const Session &session, long long asr_ms) {
    long long interval_ms = static_cast<long long>(session.chunk_seconds) * 1000;
    if (interval_ms <= 0 || asr_ms <= interval_ms) return;
    std::ostringstream event;
    event << "{\"type\":\"status\","
          << "\"session_id\":" << json_string(session.session_id) << ","
          << "\"channel_id\":" << json_string(session.channel_id) << ","
          << "\"status\":\"backpressure\","
          << "\"asr_latency_ms\":" << asr_ms << ","
          << "\"asr_interval_ms\":" << interval_ms << "}";
    broadcast(event.str());
}

void emit_asr_drop(const Session &session, size_t queue_depth) {
    std::ostringstream event;
    event << "{\"type\":\"status\","
          << "\"session_id\":" << json_string(session.session_id) << ","
          << "\"channel_id\":" << json_string(session.channel_id) << ","
          << "\"status\":\"backpressure\","
          << "\"reason\":\"asr_queue_full\","
          << "\"queue_depth\":" << queue_depth << "}";
    broadcast(event.str());
}

std::optional<double> json_number(const std::string &body, const std::string &key) {
    std::regex pattern("\"" + key + "\"\\s*:\\s*(-?[0-9]+(?:\\.[0-9]+)?)");
    std::smatch match;
    if (std::regex_search(body, match, pattern)) return std::stod(match[1].str());
    return std::nullopt;
}

std::string json_string_response(const std::string &body, const std::string &key) {
    auto value = json_field(body, key);
    return value.value_or("");
}

struct AsrJob {
    std::string session_id;
    std::string channel_id;
    std::string kind = "live";
    size_t sequence = 0;
    int chunk_seconds = 0;
    fs::path wav_path;
    Clock::time_point bucket_start;
    Clock::time_point bucket_end;
    Clock::time_point audio_started_at;
    Clock::time_point audio_ended_at;
    std::string language = "en";
    std::string original_live_text;
    std::vector<Segment> original_live_segments;
    std::vector<fs::path> retained_audio_chunks;
};

struct AsrResult {
    std::string session_id;
    std::string channel_id;
    std::string kind = "live";
    size_t sequence = 0;
    int chunk_seconds = 0;
    fs::path wav_path;
    Clock::time_point bucket_start;
    Clock::time_point bucket_end;
    Clock::time_point audio_started_at;
    Clock::time_point audio_ended_at;
    std::string language = "en";
    std::string original_live_text;
    std::vector<Segment> original_live_segments;
    std::vector<fs::path> retained_audio_chunks;
    std::string text;
    std::vector<Segment> segments;
    long long latency_ms = 0;
    std::string error;
};

struct AsrWorkerSnapshot {
    bool running = false;
    bool busy = false;
    bool model_loaded = false;
    size_t queue_depth = 0;
    int max_queue_depth = 0;
    long long decoded_chunks = 0;
    long long dropped_chunks = 0;
    long long last_decode_latency_ms = 0;
    long long avg_decode_latency_ms = 0;
    std::string backend;
    std::string error;
};

uint32_t read_u32_le(std::ifstream &in) {
    unsigned char bytes[4] = {0, 0, 0, 0};
    in.read(reinterpret_cast<char *>(bytes), 4);
    return static_cast<uint32_t>(bytes[0]) |
           (static_cast<uint32_t>(bytes[1]) << 8) |
           (static_cast<uint32_t>(bytes[2]) << 16) |
           (static_cast<uint32_t>(bytes[3]) << 24);
}

uint16_t read_u16_le(std::ifstream &in) {
    unsigned char bytes[2] = {0, 0};
    in.read(reinterpret_cast<char *>(bytes), 2);
    return static_cast<uint16_t>(bytes[0]) | (static_cast<uint16_t>(bytes[1]) << 8);
}

void write_u32_le(std::ofstream &out, uint32_t value) {
    unsigned char bytes[4] = {
        static_cast<unsigned char>(value & 0xff),
        static_cast<unsigned char>((value >> 8) & 0xff),
        static_cast<unsigned char>((value >> 16) & 0xff),
        static_cast<unsigned char>((value >> 24) & 0xff),
    };
    out.write(reinterpret_cast<const char *>(bytes), 4);
}

void write_u16_le(std::ofstream &out, uint16_t value) {
    unsigned char bytes[2] = {
        static_cast<unsigned char>(value & 0xff),
        static_cast<unsigned char>((value >> 8) & 0xff),
    };
    out.write(reinterpret_cast<const char *>(bytes), 2);
}

bool read_pcm16_wav(const fs::path &wav_path, std::vector<float> *pcm, std::string *error) {
    std::ifstream in(wav_path, std::ios::binary);
    if (!in) {
        *error = "failed to open wav";
        return false;
    }

    char riff[4] = {};
    char wave[4] = {};
    in.read(riff, 4);
    (void)read_u32_le(in);
    in.read(wave, 4);
    if (std::strncmp(riff, "RIFF", 4) != 0 || std::strncmp(wave, "WAVE", 4) != 0) {
        *error = "invalid wav header";
        return false;
    }

    bool saw_fmt = false;
    uint16_t audio_format = 0;
    uint16_t channels = 0;
    uint32_t sample_rate = 0;
    uint16_t bits_per_sample = 0;
    std::vector<char> data;

    while (in && !in.eof()) {
        char chunk_id[4] = {};
        in.read(chunk_id, 4);
        if (in.gcount() != 4) break;
        uint32_t chunk_size = read_u32_le(in);
        std::string id(chunk_id, 4);
        if (id == "fmt ") {
            audio_format = read_u16_le(in);
            channels = read_u16_le(in);
            sample_rate = read_u32_le(in);
            (void)read_u32_le(in);
            (void)read_u16_le(in);
            bits_per_sample = read_u16_le(in);
            if (chunk_size > 16) in.seekg(static_cast<std::streamoff>(chunk_size - 16), std::ios::cur);
            saw_fmt = true;
        } else if (id == "data") {
            data.resize(chunk_size);
            in.read(data.data(), static_cast<std::streamsize>(data.size()));
        } else {
            in.seekg(static_cast<std::streamoff>(chunk_size), std::ios::cur);
        }
        if (chunk_size % 2 == 1) in.seekg(1, std::ios::cur);
    }

    if (!saw_fmt || data.empty()) {
        *error = "wav missing fmt or data chunk";
        return false;
    }
    if (audio_format != 1 || channels != 1 || sample_rate != 16000 || bits_per_sample != 16) {
        std::ostringstream out;
        out << "unsupported wav format: format=" << audio_format
            << " channels=" << channels
            << " sample_rate=" << sample_rate
            << " bits=" << bits_per_sample;
        *error = out.str();
        return false;
    }

    pcm->clear();
    pcm->reserve(data.size() / 2);
    for (size_t i = 0; i + 1 < data.size(); i += 2) {
        uint16_t raw = static_cast<unsigned char>(data[i]) |
                       (static_cast<uint16_t>(static_cast<unsigned char>(data[i + 1])) << 8);
        int16_t sample = static_cast<int16_t>(raw);
        pcm->push_back(static_cast<float>(sample) / 32768.0f);
    }
    return true;
}

bool write_pcm16_wav(const fs::path &wav_path, const std::vector<float> &pcm, std::string *error) {
    std::ofstream out(wav_path, std::ios::binary);
    if (!out) {
        *error = "failed to create repair wav";
        return false;
    }

    uint32_t data_size = static_cast<uint32_t>(pcm.size() * sizeof(int16_t));
    out.write("RIFF", 4);
    write_u32_le(out, 36 + data_size);
    out.write("WAVE", 4);
    out.write("fmt ", 4);
    write_u32_le(out, 16);
    write_u16_le(out, 1);
    write_u16_le(out, 1);
    write_u32_le(out, 16000);
    write_u32_le(out, 16000 * 2);
    write_u16_le(out, 2);
    write_u16_le(out, 16);
    out.write("data", 4);
    write_u32_le(out, data_size);

    for (float sample : pcm) {
        float clamped = std::max(-1.0f, std::min(1.0f, sample));
        int16_t value = static_cast<int16_t>(clamped * 32767.0f);
        uint16_t raw = static_cast<uint16_t>(value);
        unsigned char bytes[2] = {
            static_cast<unsigned char>(raw & 0xff),
            static_cast<unsigned char>((raw >> 8) & 0xff),
        };
        out.write(reinterpret_cast<const char *>(bytes), 2);
    }
    if (!out) {
        *error = "failed to write repair wav";
        return false;
    }
    return true;
}

std::string transcribe_wav_nvidia_hosted(const fs::path &wav_path, long long *latency_ms, std::string *error) {
    auto started = Clock::now();
    if (!env_present("NVIDIA_API_KEY")) {
        *latency_ms = ms_between(started, Clock::now());
        *error = "NVIDIA_API_KEY is required";
        return "";
    }
    if (g_config.nvidia_function_id.empty()) {
        *latency_ms = ms_between(started, Clock::now());
        *error = "NVIDIA_NIM_ASR_FUNCTION_ID is required";
        return "";
    }
    if (!fs::exists(g_config.nvidia_helper)) {
        *latency_ms = ms_between(started, Clock::now());
        *error = "NVIDIA ASR helper not found: " + g_config.nvidia_helper;
        return "";
    }

    std::ostringstream command;
    command << shell_quote(g_config.nvidia_python)
            << " " << shell_quote(g_config.nvidia_helper)
            << " --input-file " << shell_quote(wav_path.string())
            << " --server " << shell_quote(g_config.nvidia_server)
            << " --function-id " << shell_quote(g_config.nvidia_function_id)
            << " --language-code " << shell_quote(g_config.nvidia_language_code)
            << " --file-streaming-chunk " << g_config.nvidia_file_streaming_chunk;
    if (!g_config.nvidia_model_name.empty()) {
        command << " --model-name " << shell_quote(g_config.nvidia_model_name);
    }
    command << " 2>/dev/null";

    std::string text = run_capture(command.str());
    *latency_ms = ms_between(started, Clock::now());
    text = trim(text);
    if (text.empty()) *error = "NVIDIA ASR returned empty transcript";
    return text;
}

bool nvidia_python_ready() {
    std::string command = shell_quote(g_config.nvidia_python) + " -c 'import riva.client' >/dev/null 2>&1";
    return std::system(command.c_str()) == 0;
}

class AsrWorker {
public:
    void start(int max_queue_depth = 0) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (running_) return;
        running_ = true;
        stopping_ = false;
        max_queue_depth_ = std::max(1, max_queue_depth > 0 ? max_queue_depth : g_config.asr_queue_max_depth);
        worker_ = std::thread(&AsrWorker::run, this);
    }

    void stop() {
        {
            std::lock_guard<std::mutex> lock(mutex_);
            if (!running_) return;
            stopping_ = true;
        }
        cv_.notify_all();
        if (worker_.joinable()) worker_.join();
        std::lock_guard<std::mutex> lock(mutex_);
        running_ = false;
    }

    bool submit(const AsrJob &job) {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!running_ || stopping_) {
            dropped_chunks_++;
            return false;
        }
        if (queue_.size() >= static_cast<size_t>(max_queue_depth_)) {
            dropped_chunks_++;
            return false;
        }
        queue_.push_back(job);
        cv_.notify_one();
        return true;
    }

    bool wait_result(const std::string &session_id, AsrResult *result, std::shared_ptr<std::atomic_bool> stop, std::chrono::milliseconds timeout) {
        std::unique_lock<std::mutex> lock(mutex_);
        auto has_result = [&]() {
            return find_result_locked(session_id) != results_.end() || stopping_ || (stop && stop->load());
        };
        if (!result_cv_.wait_for(lock, timeout, has_result)) return false;
        auto found = find_result_locked(session_id);
        if (found == results_.end()) return false;
        *result = *found;
        results_.erase(found);
        return true;
    }

    bool try_pop_result(const std::string &session_id, AsrResult *result) {
        std::lock_guard<std::mutex> lock(mutex_);
        auto found = find_result_locked(session_id);
        if (found == results_.end()) return false;
        *result = *found;
        results_.erase(found);
        return true;
    }

    size_t cancel_session(const std::string &session_id) {
        std::lock_guard<std::mutex> lock(mutex_);
        size_t removed = 0;
        auto queue_it = queue_.begin();
        while (queue_it != queue_.end()) {
            if (queue_it->session_id == session_id) {
                queue_it = queue_.erase(queue_it);
                removed++;
            } else {
                ++queue_it;
            }
        }
        auto result_it = results_.begin();
        while (result_it != results_.end()) {
            if (result_it->session_id == session_id) {
                result_it = results_.erase(result_it);
                removed++;
            } else {
                ++result_it;
            }
        }
        return removed;
    }

    void clear_results() {
        std::lock_guard<std::mutex> lock(mutex_);
        results_.clear();
    }

    AsrWorkerSnapshot snapshot() {
        std::lock_guard<std::mutex> lock(mutex_);
        AsrWorkerSnapshot snapshot;
        snapshot.running = running_;
        snapshot.busy = busy_;
        snapshot.model_loaded = model_loaded_;
        snapshot.queue_depth = queue_.size();
        snapshot.max_queue_depth = max_queue_depth_;
        snapshot.decoded_chunks = decoded_chunks_;
        snapshot.dropped_chunks = dropped_chunks_;
        snapshot.last_decode_latency_ms = last_decode_latency_ms_;
        snapshot.avg_decode_latency_ms = decoded_chunks_ > 0 ? total_decode_latency_ms_ / decoded_chunks_ : 0;
        snapshot.backend = backend_;
        snapshot.error = error_;
        return snapshot;
    }

private:
    std::deque<AsrResult>::iterator find_result_locked(const std::string &session_id) {
        return std::find_if(results_.begin(), results_.end(), [&](const AsrResult &result) {
            return result.session_id == session_id;
        });
    }

    void run() {
        initialize_backend();
        while (true) {
            AsrJob job;
            {
                std::unique_lock<std::mutex> lock(mutex_);
                cv_.wait(lock, [&] { return stopping_ || !queue_.empty(); });
                if (stopping_ && queue_.empty()) break;
                job = queue_.front();
                queue_.pop_front();
                busy_ = true;
            }

            AsrResult result;
            result.session_id = job.session_id;
            result.channel_id = job.channel_id;
            result.kind = job.kind;
            result.sequence = job.sequence;
            result.chunk_seconds = job.chunk_seconds;
            result.wav_path = job.wav_path;
            result.bucket_start = job.bucket_start;
            result.bucket_end = job.bucket_end;
            result.audio_started_at = job.audio_started_at;
            result.audio_ended_at = job.audio_ended_at;
            result.language = job.language;
            result.original_live_text = job.original_live_text;
            result.original_live_segments = job.original_live_segments;
            result.retained_audio_chunks = job.retained_audio_chunks;
            result.text = decode(job.wav_path, job.chunk_seconds, &result.latency_ms, &result.segments, &result.error);

            {
                std::lock_guard<std::mutex> lock(mutex_);
                busy_ = false;
                decoded_chunks_++;
                last_decode_latency_ms_ = result.latency_ms;
                total_decode_latency_ms_ += result.latency_ms;
                results_.push_back(result);
            }
            result_cv_.notify_all();
        }
    }

    void initialize_backend() {
        std::lock_guard<std::mutex> lock(mutex_);
        if (!using_nvidia_asr()) {
            backend_ = "unsupported-asr-provider";
            model_loaded_ = false;
            error_ = "unsupported TRANSCRIPT_ASR_PROVIDER: " + normalized_asr_provider();
            return;
        }

        bool has_key = env_present("NVIDIA_API_KEY");
        bool has_function = !g_config.nvidia_function_id.empty();
        std::string helper = using_nvidia_streaming_asr() ? g_config.nvidia_streaming_helper : g_config.nvidia_helper;
        bool helper_exists = fs::exists(helper);
        bool python_ready = nvidia_python_ready();
        backend_ = using_nvidia_streaming_asr() ? "nvidia-nim-streaming" : "nvidia-nim-hosted";
        model_loaded_ = has_key && has_function && helper_exists && python_ready;
        if (!has_key) {
            error_ = "NVIDIA_API_KEY is required";
        } else if (!has_function) {
            error_ = "NVIDIA_NIM_ASR_FUNCTION_ID is required";
        } else if (!helper_exists) {
            error_ = "NVIDIA ASR helper not found: " + helper;
        } else if (!python_ready) {
            error_ = "Python environment is missing nvidia-riva-client";
        } else {
            error_.clear();
        }
    }

    std::string decode(const fs::path &wav_path, int chunk_seconds, long long *latency_ms, std::vector<Segment> *segments, std::string *error) {
        if (using_nvidia_asr()) {
            std::string text = transcribe_wav_nvidia_hosted(wav_path, latency_ms, error);
            if (!text.empty()) segments->push_back(Segment{0, static_cast<double>(chunk_seconds), text, 0.86});
            return text;
        }
        auto started = Clock::now();
        *error = "unsupported TRANSCRIPT_ASR_PROVIDER: " + normalized_asr_provider();
        *latency_ms = ms_between(started, Clock::now());
        return "";
    }

    std::mutex mutex_;
    std::condition_variable cv_;
    std::condition_variable result_cv_;
    std::deque<AsrJob> queue_;
    std::deque<AsrResult> results_;
    std::thread worker_;
    bool running_ = false;
    bool stopping_ = false;
    bool busy_ = false;
    bool model_loaded_ = false;
    int max_queue_depth_ = 4;
    long long decoded_chunks_ = 0;
    long long dropped_chunks_ = 0;
    long long last_decode_latency_ms_ = 0;
    long long total_decode_latency_ms_ = 0;
    std::string backend_ = "starting";
    std::string error_;
};

AsrWorker g_asr_worker;
AsrWorker g_repair_worker;

void apply_sentiment(Bucket &bucket) {
    if (g_config.analyzer_url.empty() || bucket.text.empty()) return;
    auto started = Clock::now();
    fs::path temp = fs::temp_directory_path() / ("transcript-sentiment-" + bucket.session_id + ".json");
    {
        std::ofstream payload(temp);
        payload << "{\"session_id\":" << json_string(bucket.session_id)
                << ",\"channel_id\":" << json_string(bucket.channel_id)
                << ",\"bucket_start\":" << json_string(iso_time(bucket.bucket_start))
                << ",\"bucket_end\":" << json_string(iso_time(bucket.bucket_end))
                << ",\"messages\":[{\"message_id\":" << json_string(bucket.session_id + ":" + iso_time(bucket.bucket_start))
                << ",\"timestamp\":" << json_string(iso_time(bucket.bucket_start))
                << ",\"username\":\"streamer\",\"display_name\":" << json_string(bucket.channel_id)
                << ",\"text\":" << json_string(bucket.text) << "}]}";
    }
    std::string command = "curl -fsS --max-time " + std::to_string(g_config.analyzer_timeout_seconds) +
                          " -H 'Content-Type: application/json' --data @" + shell_quote(temp.string()) + " " +
                          shell_quote(g_config.analyzer_url + "/analyze/chat-bucket") + " 2>/dev/null";
    std::string response = run_capture(command);
    fs::remove(temp);
    bucket.sentiment_latency_ms = ms_between(started, Clock::now());
    if (response.empty()) {
        bucket.sentiment_status = "unavailable";
        return;
    }
    bucket.sentiment_score = json_number(response, "sentiment_score");
    bucket.sentiment_confidence = json_number(response, "confidence");
    bucket.sentiment_model = json_string_response(response, "model");
    bucket.sentiment_status = "python";
    std::regex label_pattern("\"label\"\\s*:\\s*\"([^\"]*)\"");
    std::smatch match;
    if (std::regex_search(response, match, label_pattern)) bucket.sentiment_label = match[1].str();
}

void record_segment(const TranscriptUpdate &update) {
    std::string segment_event = update_json(update);
    {
        std::lock_guard<std::mutex> lock(g_mutex);
        if (!g_session || g_session->session_id != update.session_id) return;
        g_session->latest_segment = update;
        g_session->segments.insert(g_session->segments.begin(), update);
        if (g_session->segments.size() > 120) g_session->segments.resize(120);
        g_session->segment_count++;
    }
    broadcast(segment_event);
}

void record_bucket(Bucket bucket) {
    std::string event = bucket_json(bucket);
    {
        std::lock_guard<std::mutex> lock(g_mutex);
        if (!g_session || g_session->session_id != bucket.session_id) return;
        g_session->latest_bucket = bucket;
        auto found = std::find_if(g_session->buckets.begin(), g_session->buckets.end(), [&](const Bucket &existing) {
            return existing.session_id == bucket.session_id &&
                   existing.channel_id == bucket.channel_id &&
                   existing.bucket_start == bucket.bucket_start &&
                   existing.bucket_end == bucket.bucket_end;
        });
        if (found != g_session->buckets.end()) {
            *found = bucket;
        } else {
            g_session->buckets.insert(g_session->buckets.begin(), bucket);
            if (g_session->buckets.size() > 120) g_session->buckets.resize(120);
            g_session->bucket_count++;
        }
    }
    broadcast(event);
}

std::vector<fs::path> list_wavs(const fs::path &dir) {
    std::vector<fs::path> result;
    if (!fs::exists(dir)) return result;
    for (const auto &entry : fs::directory_iterator(dir)) {
        if (entry.is_regular_file() && entry.path().extension() == ".wav" &&
            entry.path().filename().string().rfind("chunk_", 0) == 0) {
            result.push_back(entry.path());
        }
    }
    std::sort(result.begin(), result.end());
    return result;
}

Clock::time_point audio_offset_time(Clock::time_point audio_start, double offset_seconds) {
    double clamped = std::max(0.0, offset_seconds);
    return audio_start + std::chrono::milliseconds(static_cast<long long>(clamped * 1000.0));
}

std::string nvidia_streaming_command(const Session &session) {
    std::ostringstream command;
    command << shell_quote(g_config.nvidia_python)
            << " " << shell_quote(g_config.nvidia_streaming_helper)
            << " --twitch-url " << shell_quote(session.twitch_url)
            << " --session-id " << shell_quote(session.session_id)
            << " --channel-id " << shell_quote(session.channel_id)
            << " --server " << shell_quote(g_config.nvidia_server)
            << " --function-id " << shell_quote(g_config.nvidia_function_id)
            << " --language-code " << shell_quote(g_config.nvidia_language_code)
            << " --file-streaming-chunk " << g_config.nvidia_file_streaming_chunk;
    if (!g_config.nvidia_model_name.empty()) {
        command << " --model-name " << shell_quote(g_config.nvidia_model_name);
    }
    return command.str();
}

void run_nvidia_streaming_session(Session session, std::shared_ptr<std::atomic_bool> stop) {
    set_status("ingesting");
    if (!env_present("NVIDIA_API_KEY")) {
        set_status("error", "NVIDIA_API_KEY is required");
        return;
    }
    if (g_config.nvidia_function_id.empty()) {
        set_status("error", "NVIDIA_NIM_ASR_FUNCTION_ID is required");
        return;
    }
    if (!fs::exists(g_config.nvidia_streaming_helper)) {
        set_status("error", "NVIDIA ASR streaming helper not found: " + g_config.nvidia_streaming_helper);
        return;
    }
    if (!nvidia_python_ready()) {
        set_status("error", "Python environment is missing nvidia-riva-client");
        return;
    }

    int output_fd = -1;
    pid_t streaming_pid = start_process_group_capture(nvidia_streaming_command(session), &output_fd);
    if (streaming_pid <= 0 || output_fd < 0) {
        if (output_fd >= 0) close(output_fd);
        set_status("error", "failed to start NVIDIA streaming ASR helper");
        return;
    }

    auto audio_start = Clock::now();
    std::optional<Bucket> active_bucket;
    bool got_segment = false;
    bool had_error = false;
    bool helper_exited = false;

    auto finalize_bucket = [&]() {
        if (!active_bucket) return;
        Bucket bucket = *active_bucket;
        bucket.transcribed_at = Clock::now();
        bucket.pipeline_latency_ms = std::max(0LL, ms_between(bucket.audio_ended_at, bucket.transcribed_at));
        bucket.transcript_confidence = bucket.text.empty() ? 0.0 : bucket.transcript_confidence;
        bucket.repair_status = "disabled";
        apply_sentiment(bucket);
        record_bucket(bucket);
        active_bucket.reset();
    };

    auto process_line = [&](const std::string &raw_line) {
        std::string line = trim(raw_line);
        if (line.empty()) return;

        std::string type = json_field(line, "type").value_or("");
        if (type == "status") {
            broadcast(line);
            return;
        }
        if (type == "transcript_partial") {
            {
                std::lock_guard<std::mutex> lock(g_mutex);
                if (g_session && g_session->session_id == session.session_id) g_session->partial_count++;
            }
            broadcast(line);
            return;
        }
        if (type == "error") {
            had_error = true;
            stop->store(true);
            set_status("error", json_field(line, "error").value_or("NVIDIA streaming ASR failed"));
            return;
        }
        if (type != "transcript_segment") return;

        std::string text = trim(json_field(line, "text").value_or(""));
        if (text.empty()) return;

        double segment_start_seconds = json_number(line, "audio_start_seconds").value_or(0.0);
        double segment_end_seconds = json_number(line, "audio_end_seconds").value_or(segment_start_seconds);
        if (segment_end_seconds <= segment_start_seconds) segment_end_seconds = segment_start_seconds + 0.1;
        double confidence = json_number(line, "confidence").value_or(0.86);
        long long asr_latency_ms = static_cast<long long>(json_number(line, "asr_latency_ms").value_or(0.0));

        auto segment_start = audio_offset_time(audio_start, segment_start_seconds);
        auto segment_end = audio_offset_time(audio_start, segment_end_seconds);
        auto transcribed_at = Clock::now();

        TranscriptUpdate update;
        update.session_id = session.session_id;
        update.channel_id = session.channel_id;
        update.transcript_start = segment_start;
        update.transcript_end = segment_end;
        update.audio_started_at = segment_start;
        update.audio_ended_at = segment_end;
        update.transcribed_at = transcribed_at;
        update.text = text;
        update.language = transcript_language_code();
        update.transcript_confidence = confidence;
        update.asr_latency_ms = asr_latency_ms;
        update.pipeline_latency_ms = std::max(0LL, ms_between(segment_end, transcribed_at));
        update.segments.push_back(Segment{0, duration_seconds(segment_start, segment_end), text, confidence});
        record_segment(update);
        got_segment = true;

        long long bucket_index = static_cast<long long>(std::max(0.0, segment_start_seconds)) / session.bucket_seconds;
        auto bucket_start = audio_start + std::chrono::seconds(bucket_index * session.bucket_seconds);
        if (!active_bucket || active_bucket->bucket_start != bucket_start) {
            finalize_bucket();
            Bucket bucket;
            bucket.session_id = session.session_id;
            bucket.channel_id = session.channel_id;
            bucket.bucket_start = bucket_start;
            bucket.bucket_end = bucket_start + std::chrono::seconds(session.bucket_seconds);
            bucket.audio_started_at = segment_start;
            bucket.audio_ended_at = segment_start;
            bucket.language = update.language;
            bucket.sentiment_status = "skipped";
            bucket.transcript_confidence = confidence;
            bucket.asr_latency_ms = asr_latency_ms;
            bucket.repair_status = "disabled";
            active_bucket = bucket;
        }

        if (active_bucket) {
            if (!active_bucket->text.empty()) active_bucket->text += " ";
            active_bucket->text += text;
            active_bucket->audio_ended_at = std::max(active_bucket->audio_ended_at, segment_end);
            active_bucket->asr_latency_ms = asr_latency_ms;
            active_bucket->transcript_confidence = std::max(active_bucket->transcript_confidence, confidence);
            active_bucket->segments.push_back(Segment{
                duration_seconds(active_bucket->bucket_start, segment_start),
                duration_seconds(active_bucket->bucket_start, segment_end),
                text,
                confidence
            });
            if (segment_end >= active_bucket->bucket_end) finalize_bucket();
        }
    };

    std::string pending;
    char buffer[4096];
    while (!stop->load()) {
        pollfd pfd{};
        pfd.fd = output_fd;
        pfd.events = POLLIN | POLLHUP | POLLERR;
        int poll_result = poll(&pfd, 1, 500);
        if (poll_result < 0) {
            if (errno == EINTR) continue;
            had_error = true;
            set_status("error", "failed to read NVIDIA streaming ASR output");
            break;
        }

        if (poll_result > 0 && (pfd.revents & (POLLIN | POLLHUP | POLLERR))) {
            while (true) {
                ssize_t n = read(output_fd, buffer, sizeof(buffer));
                if (n > 0) {
                    pending.append(buffer, buffer + n);
                    size_t newline = std::string::npos;
                    while ((newline = pending.find('\n')) != std::string::npos) {
                        std::string line = pending.substr(0, newline);
                        pending.erase(0, newline + 1);
                        process_line(line);
                    }
                    continue;
                }
                if (n == 0) {
                    if (!pending.empty()) {
                        process_line(pending);
                        pending.clear();
                    }
                    helper_exited = true;
                    break;
                }
                if (errno == EAGAIN || errno == EWOULDBLOCK) break;
                had_error = true;
                set_status("error", "failed to read NVIDIA streaming ASR output");
                stop->store(true);
                break;
            }
        }
        if (helper_exited) break;

        int status = 0;
        pid_t exited = waitpid(streaming_pid, &status, WNOHANG);
        if (exited == streaming_pid) {
            if (!pending.empty()) {
                process_line(pending);
                pending.clear();
            }
            helper_exited = true;
            break;
        }
    }

    stop_process_group(streaming_pid);
    close(output_fd);
    finalize_bucket();

    if (!had_error) {
        if (!got_segment && !stop->load()) {
            set_status("error", "NVIDIA streaming ASR exited before producing transcript segments");
        } else {
            set_status("stopped");
        }
    }
}

void run_session(Session session, std::shared_ptr<std::atomic_bool> stop) {
    if (using_nvidia_streaming_asr()) {
        run_nvidia_streaming_session(session, stop);
        return;
    }

    set_status("ingesting");
    fs::path work_dir = fs::temp_directory_path() / ("transcript-cpp-" + session.session_id);
    fs::remove_all(work_dir);
    fs::create_directories(work_dir);
    fs::path pattern = work_dir / "chunk_%06d.wav";
    std::string command = "streamlink --stdout " + shell_quote(session.twitch_url) + " audio_only,best"
        + " | ffmpeg -hide_banner -loglevel error -i pipe:0 -vn -ar 16000 -ac 1 -c:a pcm_s16le"
        + " -f segment -segment_time " + std::to_string(session.chunk_seconds)
        + " -reset_timestamps 1 " + shell_quote(pattern.string());
    pid_t media_pid = start_process_group(command);
    std::set<fs::path> processed;
    auto audio_start = Clock::now();
    std::optional<Bucket> active_bucket;
    size_t accepted_jobs = 0;
    size_t completed_jobs = 0;
    size_t accepted_repairs = 0;
    size_t completed_repairs = 0;

    auto cleanup_paths = [](const std::vector<fs::path> &paths) {
        for (const auto &path : paths) fs::remove(path);
    };

    auto enqueue_repair = [&](Bucket &bucket) {
        if (!g_config.repair_enabled) {
            bucket.repair_status = "disabled";
            return false;
        }
        if (bucket.retained_audio_chunks.empty()) {
            bucket.repair_status = "no_audio";
            return false;
        }

        std::vector<float> pcm;
        std::string error;
        for (const auto &chunk : bucket.retained_audio_chunks) {
            std::vector<float> chunk_pcm;
            if (!read_pcm16_wav(chunk, &chunk_pcm, &error)) {
                bucket.repair_status = "audio_read_failed";
                return false;
            }
            pcm.insert(pcm.end(), chunk_pcm.begin(), chunk_pcm.end());
        }
        if (pcm.empty()) {
            bucket.repair_status = "no_audio";
            return false;
        }

        fs::path repair_wav = work_dir / ("repair_" + std::to_string(bucket.bucket_start.time_since_epoch().count()) + ".wav");
        if (!write_pcm16_wav(repair_wav, pcm, &error)) {
            bucket.repair_status = "audio_write_failed";
            return false;
        }

        AsrJob job;
        job.session_id = bucket.session_id;
        job.channel_id = bucket.channel_id;
        job.kind = "repair";
        job.sequence = accepted_repairs;
        job.chunk_seconds = static_cast<int>(std::max(1.0, duration_seconds(bucket.audio_started_at, bucket.audio_ended_at)));
        job.wav_path = repair_wav;
        job.bucket_start = bucket.bucket_start;
        job.bucket_end = bucket.bucket_end;
        job.audio_started_at = bucket.audio_started_at;
        job.audio_ended_at = bucket.audio_ended_at;
        job.language = bucket.language;
        job.original_live_text = bucket.text;
        job.original_live_segments = bucket.segments;
        job.retained_audio_chunks = bucket.retained_audio_chunks;
        if (!g_repair_worker.submit(job)) {
            fs::remove(repair_wav);
            bucket.repair_status = "queue_full";
            return false;
        }

        accepted_repairs++;
        bucket.repair_status = "pending";
        return true;
    };

    auto finalize_bucket = [&]() {
        if (!active_bucket) return;
        Bucket bucket = *active_bucket;
        bucket.transcribed_at = Clock::now();
        bucket.pipeline_latency_ms = ms_between(bucket.audio_ended_at, bucket.transcribed_at);
        bucket.transcript_confidence = bucket.text.empty() ? 0.0 : 0.80;
        apply_sentiment(bucket);
        bucket.repair_status = g_config.repair_enabled ? "pending" : "disabled";
        record_bucket(bucket);
        bool repair_queued = enqueue_repair(bucket);
        if (!repair_queued && bucket.repair_status != "pending") {
            record_bucket(bucket);
        }
        if (!repair_queued) cleanup_paths(bucket.retained_audio_chunks);
        active_bucket.reset();
    };

    auto handle_repair_result = [&](const AsrResult &result) {
        completed_repairs++;
        auto transcribed_at = Clock::now();
        std::string repaired_text = trim(result.text);

        Bucket bucket;
        bucket.session_id = result.session_id;
        bucket.channel_id = result.channel_id;
        bucket.bucket_start = result.bucket_start;
        bucket.bucket_end = result.bucket_end;
        bucket.audio_started_at = result.audio_started_at;
        bucket.audio_ended_at = result.audio_ended_at;
        bucket.transcribed_at = transcribed_at;
        bucket.text = repaired_text.empty() ? result.original_live_text : repaired_text;
        bucket.language = result.language.empty() ? transcript_language_code() : result.language;
        bucket.transcript_confidence = bucket.text.empty() ? 0.0 : 0.86;
        bucket.asr_latency_ms = result.latency_ms;
        bucket.pipeline_latency_ms = ms_between(bucket.audio_ended_at, transcribed_at);
        bucket.repaired = true;
        bucket.repair_status = repaired_text.empty() ? (result.error.empty() ? "empty" : "failed") : "completed";
        bucket.repair_latency_ms = result.latency_ms;
        bucket.original_live_text = result.original_live_text;
        bucket.segments = repaired_text.empty() ? result.original_live_segments : result.segments;
        apply_sentiment(bucket);
        record_bucket(bucket);
        fs::remove(result.wav_path);
        cleanup_paths(result.retained_audio_chunks);
    };

    auto drain_repair_results = [&]() {
        AsrResult result;
        while (g_repair_worker.try_pop_result(session.session_id, &result)) {
            handle_repair_result(result);
        }
    };

    auto handle_asr_result = [&](const AsrResult &result) {
        size_t index = result.sequence;
        auto chunk_start = audio_start + std::chrono::seconds(static_cast<long long>(index * session.chunk_seconds));
        auto chunk_end = chunk_start + std::chrono::seconds(session.chunk_seconds);
        auto transcribed_at = Clock::now();
        std::string text = trim(result.text);

        TranscriptUpdate update;
        update.session_id = session.session_id;
        update.channel_id = session.channel_id;
        update.transcript_start = chunk_start;
        update.transcript_end = chunk_end;
        update.audio_started_at = chunk_start;
        update.audio_ended_at = chunk_end;
        update.transcribed_at = transcribed_at;
        update.text = text;
        update.language = transcript_language_code();
        update.transcript_confidence = text.empty() ? 0.0 : 0.80;
        update.asr_latency_ms = result.latency_ms;
        update.pipeline_latency_ms = ms_between(chunk_end, transcribed_at);
        update.segments = result.segments;
        record_segment(update);
        emit_asr_backpressure(session, result.latency_ms);

        long long chunk_offset = static_cast<long long>(index * session.chunk_seconds);
        long long bucket_offset = (chunk_offset / session.bucket_seconds) * session.bucket_seconds;
        auto bucket_start = audio_start + std::chrono::seconds(bucket_offset);
        if (!active_bucket || active_bucket->bucket_start != bucket_start) {
            finalize_bucket();
            Bucket bucket;
            bucket.session_id = session.session_id;
            bucket.channel_id = session.channel_id;
            bucket.bucket_start = bucket_start;
            bucket.bucket_end = bucket_start + std::chrono::seconds(session.bucket_seconds);
            bucket.audio_started_at = bucket_start;
            bucket.audio_ended_at = bucket_start;
            bucket.language = update.language;
            bucket.sentiment_status = "skipped";
            bucket.asr_latency_ms = 0;
            active_bucket = bucket;
        }
        if (active_bucket) {
            if (!active_bucket->text.empty() && !text.empty()) active_bucket->text += " ";
            active_bucket->text += text;
            active_bucket->audio_ended_at = chunk_end;
            active_bucket->asr_latency_ms = active_bucket->asr_latency_ms.value_or(0) + result.latency_ms;
            active_bucket->retained_audio_chunks.push_back(result.wav_path);
            for (const auto &segment : result.segments) {
                if (segment.text.empty()) continue;
                active_bucket->segments.push_back(Segment{
                    duration_seconds(active_bucket->bucket_start, chunk_start) + segment.start,
                    duration_seconds(active_bucket->bucket_start, chunk_start) + segment.end,
                    segment.text,
                    segment.confidence
                });
            }
            if (chunk_end >= active_bucket->bucket_end) finalize_bucket();
        }
    };

    auto drain_results = [&]() {
        AsrResult result;
        while (g_asr_worker.try_pop_result(session.session_id, &result)) {
            completed_jobs++;
            handle_asr_result(result);
        }
        drain_repair_results();
    };

    auto process_wavs = [&](bool force) {
        drain_results();
        auto wavs = list_wavs(work_dir);
        for (const auto &wav : wavs) {
            if (processed.count(wav)) continue;
            if (!force) {
                auto modified = fs::last_write_time(wav);
                auto now_file = decltype(modified)::clock::now();
                if (now_file - modified < std::chrono::seconds(2)) continue;
            }

            size_t index = processed.size();
            AsrJob job;
            job.session_id = session.session_id;
            job.channel_id = session.channel_id;
            job.kind = "live";
            job.sequence = index;
            job.chunk_seconds = session.chunk_seconds;
            job.wav_path = wav;
            if (g_asr_worker.submit(job)) {
                accepted_jobs++;
            } else {
                emit_asr_drop(session, g_asr_worker.snapshot().queue_depth);
                fs::remove(wav);
            }
            processed.insert(wav);
            drain_results();
        }
    };

    while (!stop->load()) {
        int media_status = 0;
        pid_t media_result = waitpid(media_pid, &media_status, WNOHANG);
        if (media_result == media_pid) {
            if (processed.empty()) {
                set_status("error", "audio pipeline exited before producing transcript audio");
                fs::remove_all(work_dir);
                return;
            }
            break;
        }
        process_wavs(false);
        std::this_thread::sleep_for(std::chrono::milliseconds(500));
    }

    stop_process_group(media_pid);
    std::this_thread::sleep_for(std::chrono::milliseconds(500));
    if (!stop->load()) process_wavs(true);
    while (completed_jobs < accepted_jobs) {
        AsrResult result;
        if (g_asr_worker.wait_result(session.session_id, &result, nullptr, std::chrono::milliseconds(500))) {
            completed_jobs++;
            handle_asr_result(result);
        }
    }
    finalize_bucket();
    if (!stop->load()) {
        while (completed_repairs < accepted_repairs) {
            AsrResult result;
            if (g_repair_worker.wait_result(session.session_id, &result, nullptr, std::chrono::milliseconds(500))) {
                handle_repair_result(result);
            }
        }
    } else {
        g_repair_worker.cancel_session(session.session_id);
    }
    set_status("stopped");
    fs::remove_all(work_dir);
}

std::string state_json(const std::string &mode) {
    std::lock_guard<std::mutex> lock(g_mutex);
    if (!g_session) return "{\"status\":\"idle\",\"mode\":" + json_string(mode) + "}";
    const auto &session = *g_session;
    std::ostringstream out;
    out << "{"
        << "\"status\":" << json_string(session.status) << ","
        << "\"session_id\":" << json_string(session.session_id) << ","
        << "\"channel_id\":" << json_string(session.channel_id) << ","
        << "\"bucket_seconds\":" << session.bucket_seconds << ","
        << "\"chunk_seconds\":" << session.chunk_seconds << ","
        << "\"caption\":{\"rolling_window_seconds\":" << env_int("CAPTION_ROLLING_WINDOW_SECONDS", 10)
        << ",\"asr_interval_seconds\":" << env_int("CAPTION_ASR_INTERVAL_SECONDS", 1)
        << ",\"stability_passes\":1,\"rolling_buffer_seconds\":120,\"commit_lag_seconds\":0},"
        << "\"partial_count\":" << session.partial_count << ","
        << "\"bucket_count\":" << session.bucket_count << ","
        << "\"segment_count\":" << session.segment_count << ","
        << "\"error\":" << json_string(session.error) << ","
        << "\"mode\":" << json_string(mode);
    if (mode == "all" || mode == "live") {
        out << ",\"partials\":[],\"segments\":[";
        for (size_t i = 0; i < session.segments.size(); i++) {
            if (i > 0) out << ",";
            out << update_json(session.segments[i]);
        }
        out << "],\"latest_partial\":null,\"latest_segment\":";
        out << (session.latest_segment ? update_json(*session.latest_segment) : "null");
    }
    if (mode == "all" || mode == "buckets") {
        out << ",\"buckets\":[";
        for (size_t i = 0; i < session.buckets.size(); i++) {
            if (i > 0) out << ",";
            out << bucket_json(session.buckets[i]);
        }
        out << "],\"latest_bucket\":";
        out << (session.latest_bucket ? bucket_json(*session.latest_bucket) : "null");
    }
    out << "}";
    return out.str();
}

std::string health_json() {
    bool nvidia_provider = using_nvidia_asr();
    bool nvidia_streaming_provider = using_nvidia_streaming_asr();
    AsrWorkerSnapshot worker = g_asr_worker.snapshot();
    AsrWorkerSnapshot repair_worker = g_repair_worker.snapshot();
    bool worker_available = nvidia_provider && worker.model_loaded;
    bool repair_available = !g_config.repair_enabled || (nvidia_provider && repair_worker.model_loaded);
    std::string status = worker.running && worker_available ? "ok" : "degraded";
    std::vector<Bucket> buckets;
    {
        std::lock_guard<std::mutex> lock(g_mutex);
        if (g_session) buckets = g_session->buckets;
    }
    double transcript_audio_seconds = 0;
    double transcript_expected_seconds = 0;
    double transcript_empty_ratio_sum = 0;
    int transcript_word_count = 0;
    int transcript_segment_count = 0;
    int transcript_repair_added_words = 0;
    std::map<std::string, int> transcript_status_counts;
    for (const auto &bucket : buckets) {
        BucketCompleteness completeness = bucket_completeness(bucket);
        transcript_audio_seconds += completeness.audio_seconds;
        transcript_expected_seconds += std::max(0.0, duration_seconds(bucket.bucket_start, bucket.bucket_end));
        transcript_empty_ratio_sum += completeness.empty_ratio;
        transcript_word_count += completeness.word_count;
        transcript_segment_count += completeness.segment_count;
        transcript_repair_added_words += completeness.repair_added_words;
        transcript_status_counts[completeness.transcript_status]++;
    }
    double transcript_audio_coverage = transcript_expected_seconds > 0 ? transcript_audio_seconds / transcript_expected_seconds : 0;
    double transcript_empty_ratio = buckets.empty() ? 0 : transcript_empty_ratio_sum / static_cast<double>(buckets.size());
    std::ostringstream out;
    out << "{"
        << "\"status\":" << json_string(status) << ","
        << "\"default_chunk_seconds\":" << g_config.default_chunk_seconds << ","
        << "\"asr\":{"
        << "\"profile\":" << json_string(normalized_asr_provider()) << ","
        << "\"backend\":" << json_string(worker.backend) << ","
        << "\"model\":" << json_string(nvidia_asr_model_display_name()) << ","
        << "\"device\":" << json_string(nvidia_provider ? "hosted_nvidia_gpu" : "unsupported") << ","
        << "\"compute_type\":" << json_string(nvidia_provider ? (nvidia_streaming_provider ? "hosted_streaming_grpc" : "hosted_grpc") : "unsupported") << ","
        << "\"language\":" << json_string(transcript_language_code()) << ","
        << "\"num_workers\":1,"
        << "\"model_loaded\":" << (worker.model_loaded ? "true" : "false") << ","
        << "\"queue_depth\":" << worker.queue_depth << ","
        << "\"max_queue_depth\":" << worker.max_queue_depth << ","
        << "\"worker_busy\":" << (worker.busy ? "true" : "false") << ","
        << "\"decoded_chunks\":" << worker.decoded_chunks << ","
        << "\"dropped_chunks\":" << worker.dropped_chunks << ","
        << "\"last_decode_latency_ms\":" << worker.last_decode_latency_ms << ","
        << "\"avg_decode_latency_ms\":" << worker.avg_decode_latency_ms << ","
        << "\"error\":" << json_string(worker.error);
    if (nvidia_provider) {
        out << ",\"server\":" << json_string(g_config.nvidia_server)
            << ",\"function_id\":" << json_string(g_config.nvidia_function_id)
            << ",\"file_streaming_chunk\":" << g_config.nvidia_file_streaming_chunk;
        if (nvidia_streaming_provider) {
            out << ",\"streaming_helper\":" << json_string(g_config.nvidia_streaming_helper);
        }
    }
    out
        << "},"
        << "\"repair\":{"
        << "\"enabled\":" << (g_config.repair_enabled ? "true" : "false") << ","
        << "\"queue_size\":" << repair_worker.queue_depth << ","
        << "\"status\":" << json_string(repair_available ? "ready" : "degraded")
        << "},"
        << "\"transcript_summary\":{"
        << "\"bucket_count\":" << buckets.size() << ","
        << "\"audio_seconds\":" << transcript_audio_seconds << ","
        << "\"expected_audio_seconds\":" << transcript_expected_seconds << ","
        << "\"audio_coverage\":" << transcript_audio_coverage << ","
        << "\"segment_count\":" << transcript_segment_count << ","
        << "\"word_count\":" << transcript_word_count << ","
        << "\"empty_ratio\":" << transcript_empty_ratio << ","
        << "\"repair_added_words\":" << transcript_repair_added_words << ","
        << "\"status_counts\":{";
    size_t status_index = 0;
    for (const auto &[key, value] : transcript_status_counts) {
        if (status_index++ > 0) out << ",";
        out << json_string(key) << ":" << value;
    }
    out << "}"
        << "},"
        << "\"caption\":{\"rolling_window_seconds\":" << env_int("CAPTION_ROLLING_WINDOW_SECONDS", 10)
        << ",\"asr_interval_seconds\":" << env_int("CAPTION_ASR_INTERVAL_SECONDS", 5)
        << ",\"stability_passes\":1,\"rolling_buffer_seconds\":120,\"commit_lag_seconds\":0},"
        << "\"quality\":{\"min_segment_confidence\":0,\"max_segment_repeats\":999},"
        << "\"warmup\":{\"enabled\":false,\"status\":\"ready\",\"error\":\"\"}"
        << "}";
    return out.str();
}

void write_metric(std::ostringstream &out, const std::string &name, const std::string &help, const std::string &type, double value) {
    out << "# HELP " << name << " " << help << "\n";
    out << "# TYPE " << name << " " << type << "\n";
    out << name << " " << value << "\n";
}

std::string metrics_text() {
    AsrWorkerSnapshot worker = g_asr_worker.snapshot();
    AsrWorkerSnapshot repair_worker = g_repair_worker.snapshot();
    int active_sessions = 0;
    int bucket_count = 0;
    int segment_count = 0;
    {
        std::lock_guard<std::mutex> lock(g_mutex);
        if (g_session && (g_session->status == "starting" || g_session->status == "ingesting")) {
            active_sessions = 1;
        }
        if (g_session) {
            bucket_count = g_session->bucket_count;
            segment_count = g_session->segment_count;
        }
    }

    std::ostringstream out;
    write_metric(out, "transcript_ingestor_active_sessions", "Active transcript ingestion sessions.", "gauge", active_sessions);
    write_metric(out, "transcript_ingestor_current_session_buckets", "Transcript buckets retained for the current session.", "gauge", bucket_count);
    write_metric(out, "transcript_ingestor_current_session_segments", "Transcript segments retained for the current session.", "gauge", segment_count);
    write_metric(out, "transcript_ingestor_asr_queue_depth", "Current ASR worker queue depth.", "gauge", worker.queue_depth);
    write_metric(out, "transcript_ingestor_asr_dropped_chunks_total", "Audio chunks dropped before ASR processing.", "counter", worker.dropped_chunks);
    write_metric(out, "transcript_ingestor_asr_decoded_chunks_total", "Audio chunks decoded by ASR.", "counter", worker.decoded_chunks);
    write_metric(out, "transcript_ingestor_asr_last_decode_latency_ms", "Most recent ASR decode latency in milliseconds.", "gauge", worker.last_decode_latency_ms);
    write_metric(out, "transcript_ingestor_repair_queue_depth", "Current transcript repair worker queue depth.", "gauge", repair_worker.queue_depth);
    return out.str();
}

struct HttpRequest {
    std::string method;
    std::string path;
    std::string query;
    std::string body;
};

std::optional<HttpRequest> read_request(int fd) {
    std::string data;
    char buffer[4096];
    while (data.find("\r\n\r\n") == std::string::npos) {
        ssize_t n = recv(fd, buffer, sizeof(buffer), 0);
        if (n <= 0) return std::nullopt;
        data.append(buffer, buffer + n);
        if (data.size() > 1024 * 1024) return std::nullopt;
    }
    auto header_end = data.find("\r\n\r\n");
    std::string headers = data.substr(0, header_end);
    std::istringstream in(headers);
    std::string request_line;
    std::getline(in, request_line);
    if (!request_line.empty() && request_line.back() == '\r') request_line.pop_back();
    std::istringstream request_parts(request_line);
    HttpRequest request;
    std::string target;
    request_parts >> request.method >> target;
    auto query_pos = target.find('?');
    request.path = query_pos == std::string::npos ? target : target.substr(0, query_pos);
    request.query = query_pos == std::string::npos ? "" : target.substr(query_pos + 1);
    int content_length = 0;
    std::string line;
    while (std::getline(in, line)) {
        if (!line.empty() && line.back() == '\r') line.pop_back();
        auto colon = line.find(':');
        if (colon == std::string::npos) continue;
        std::string key = line.substr(0, colon);
        std::transform(key.begin(), key.end(), key.begin(), [](unsigned char c) { return std::tolower(c); });
        if (key == "content-length") content_length = std::stoi(trim(line.substr(colon + 1)));
    }
    request.body = data.substr(header_end + 4);
    while (static_cast<int>(request.body.size()) < content_length) {
        ssize_t n = recv(fd, buffer, sizeof(buffer), 0);
        if (n <= 0) break;
        request.body.append(buffer, buffer + n);
    }
    if (static_cast<int>(request.body.size()) > content_length) request.body.resize(content_length);
    return request;
}

void send_response(int fd, int status, const std::string &status_text, const std::string &body, const std::string &content_type = "application/json") {
    std::ostringstream response;
    response << "HTTP/1.1 " << status << " " << status_text << "\r\n"
             << "Access-Control-Allow-Origin: *\r\n"
             << "Access-Control-Allow-Methods: GET,POST,OPTIONS\r\n"
             << "Access-Control-Allow-Headers: Content-Type\r\n"
             << "Content-Type: " << content_type << "\r\n"
             << "Content-Length: " << body.size() << "\r\n"
             << "Connection: close\r\n\r\n"
             << body;
    std::string raw = response.str();
    send(fd, raw.data(), raw.size(), 0);
}

void handle_events(int fd) {
    auto sub = std::make_shared<Subscriber>();
    {
        std::lock_guard<std::mutex> lock(g_mutex);
        g_subscribers.push_back(sub);
    }
    std::string headers = "HTTP/1.1 200 OK\r\nAccess-Control-Allow-Origin: *\r\nContent-Type: text/event-stream\r\nCache-Control: no-cache\r\nConnection: keep-alive\r\n\r\n: connected\n\n";
    send(fd, headers.data(), headers.size(), 0);
    while (true) {
        std::unique_lock<std::mutex> lock(sub->mutex);
        if (sub->cv.wait_for(lock, std::chrono::seconds(15), [&] { return !sub->events.empty() || sub->closed; })) {
            if (sub->closed) return;
            std::string event = sub->events.front();
            sub->events.pop();
            lock.unlock();
            std::string wire = "data: " + event + "\n\n";
            if (send(fd, wire.data(), wire.size(), MSG_NOSIGNAL) < 0) return;
        } else {
            std::string keepalive = ": keepalive\n\n";
            if (send(fd, keepalive.data(), keepalive.size(), MSG_NOSIGNAL) < 0) return;
        }
    }
}

void stop_session(bool wait_for_worker = true) {
    std::shared_ptr<std::atomic_bool> stop;
    {
        std::lock_guard<std::mutex> lock(g_mutex);
        stop = g_stop;
    }
    if (stop) {
        stop->store(true);
        set_status("stopping");
    }
    if (wait_for_worker && g_worker.joinable()) g_worker.join();
}

std::string start_session(const std::string &body, int *status_code) {
    auto channel_value = json_field(body, "channel");
    auto source_url_value = json_field(body, "source_url");
    if (!source_url_value || trim(*source_url_value).empty()) {
        source_url_value = json_field(body, "url");
    }
    if (!source_url_value || trim(*source_url_value).empty()) {
        source_url_value = json_field(body, "twitch_url");
    }
    if ((!channel_value || trim(*channel_value).empty()) && (!source_url_value || trim(*source_url_value).empty())) {
        *status_code = 400;
        return "{\"error\":\"channel or stream URL is required\"}";
    }
    std::string channel_source = json_field(body, "channel_id").value_or("");
    if (trim(channel_source).empty() && channel_value) channel_source = *channel_value;
    if (trim(channel_source).empty() && source_url_value) channel_source = *source_url_value;
    std::string channel = extract_channel(channel_source);
    std::string source_input = source_url_value && !trim(*source_url_value).empty() ? *source_url_value : *channel_value;
    std::string source_url = transcript_url(source_input);
    if (channel.empty() || source_url.empty()) {
        *status_code = 400;
        return "{\"error\":\"valid Twitch or YouTube stream source is required\"}";
    }
    std::string session_id = json_field(body, "session_id").value_or(channel + "-" + std::to_string(std::time(nullptr)));
    int bucket_seconds = json_int_field(body, "bucket_seconds").value_or(30);
    int chunk_seconds = json_int_field(body, "chunk_seconds").value_or(std::min(g_config.default_chunk_seconds, bucket_seconds));
    if (bucket_seconds <= 0 || bucket_seconds > 120 || chunk_seconds <= 0 || chunk_seconds > bucket_seconds) {
        *status_code = 400;
        return "{\"error\":\"invalid bucket_seconds or chunk_seconds\"}";
    }

    stop_session();
    g_asr_worker.clear_results();
    g_repair_worker.cancel_session(session_id);
    g_repair_worker.clear_results();

    Session session;
    session.session_id = session_id;
    session.channel_id = channel;
    session.twitch_url = source_url;
    session.bucket_seconds = bucket_seconds;
    session.chunk_seconds = chunk_seconds;
    session.status = "starting";
    auto stop = std::make_shared<std::atomic_bool>(false);
    {
        std::lock_guard<std::mutex> lock(g_mutex);
        g_session = session;
        g_stop = stop;
    }
    g_worker = std::thread(run_session, session, stop);

    *status_code = 202;
    std::ostringstream out;
    out << "{\"status\":\"starting\",\"session_id\":" << json_string(session_id)
        << ",\"channel_id\":" << json_string(channel)
        << ",\"bucket_seconds\":" << bucket_seconds
        << ",\"chunk_seconds\":" << chunk_seconds << "}";
    return out.str();
}

void handle_client(int fd) {
    auto request = read_request(fd);
    if (!request) {
        close(fd);
        return;
    }
    if (request->method == "OPTIONS") {
        send_response(fd, 204, "No Content", "");
    } else if (request->method == "GET" && request->path == "/health") {
        std::string body = health_json();
        send_response(fd, body.find("\"status\":\"ok\"") != std::string::npos ? 200 : 503, body.find("\"status\":\"ok\"") != std::string::npos ? "OK" : "Service Unavailable", body);
    } else if (request->method == "GET" && request->path == "/metrics") {
        send_response(fd, 200, "OK", metrics_text(), "text/plain; version=0.0.4; charset=utf-8");
    } else if (request->method == "GET" && request->path == "/state") {
        std::string mode = request->query.find("mode=live") != std::string::npos ? "live" : request->query.find("mode=buckets") != std::string::npos ? "buckets" : "all";
        send_response(fd, 200, "OK", state_json(mode));
    } else if (request->method == "GET" && request->path == "/live") {
        send_response(fd, 200, "OK", state_json("live"));
    } else if (request->method == "GET" && request->path == "/buckets") {
        send_response(fd, 200, "OK", state_json("buckets"));
    } else if (request->method == "GET" && request->path == "/events") {
        handle_events(fd);
    } else if (request->method == "POST" && request->path == "/sessions") {
        int status_code = 202;
        std::string body = start_session(request->body, &status_code);
        send_response(fd, status_code, status_code == 202 ? "Accepted" : "Bad Request", body);
    } else if (request->method == "POST" && request->path == "/stop") {
        stop_session(false);
        send_response(fd, 202, "Accepted", "{\"status\":\"stopping\"}");
    } else {
        send_response(fd, 404, "Not Found", "{\"error\":\"not found\"}");
    }
    close(fd);
}

void load_config() {
    g_config.host = env_string("TRANSCRIPT_HOST", "0.0.0.0");
    g_config.port = env_int("TRANSCRIPT_PORT", 8092);
    g_config.default_chunk_seconds = env_int("TRANSCRIPT_DEFAULT_CHUNK_SECONDS", 1);
    g_config.asr_provider = env_string("TRANSCRIPT_ASR_PROVIDER", "nvidia_streaming");
    g_config.asr_queue_max_depth = env_int("TRANSCRIPT_ASR_QUEUE_MAX_DEPTH", 4);
    g_config.repair_enabled = env_bool("TRANSCRIPT_REPAIR_ENABLED", false);
    g_config.repair_queue_max_depth = env_int("TRANSCRIPT_REPAIR_QUEUE_MAX_DEPTH", 2);
    g_config.nvidia_python = env_string("NVIDIA_NIM_ASR_PYTHON", "python3");
    g_config.nvidia_helper = env_string("NVIDIA_NIM_ASR_HELPER", "scripts/nvidia_asr_transcribe.py");
    g_config.nvidia_streaming_helper = env_string("NVIDIA_NIM_ASR_STREAMING_HELPER", "scripts/nvidia_live_stream_asr.py");
    g_config.nvidia_server = env_string("NVIDIA_NIM_ASR_SERVER", "grpc.nvcf.nvidia.com:443");
    g_config.nvidia_function_id = env_string("NVIDIA_NIM_ASR_FUNCTION_ID", kNemotronAsrStreamingFunctionId);
    g_config.nvidia_model_name = env_string("NVIDIA_NIM_ASR_MODEL_NAME", "");
    g_config.nvidia_language_code = env_string("NVIDIA_NIM_ASR_LANGUAGE_CODE", "en-US");
    g_config.nvidia_file_streaming_chunk = env_int("NVIDIA_NIM_ASR_FILE_STREAMING_CHUNK", 1600);
    g_config.analyzer_url = env_string("NLP_ANALYZER_URL", "http://sentiment-analyzer:8091");
    g_config.analyzer_timeout_seconds = env_int("NLP_ANALYZER_TIMEOUT", 15);
}

int main() {
    signal(SIGPIPE, SIG_IGN);
    load_config();
    g_asr_worker.start();
    g_repair_worker.start(g_config.repair_queue_max_depth);
    int server_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (server_fd < 0) {
        std::cerr << "socket failed: " << std::strerror(errno) << "\n";
        return 1;
    }
    int opt = 1;
    setsockopt(server_fd, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(opt));
    sockaddr_in addr{};
    addr.sin_family = AF_INET;
    addr.sin_port = htons(static_cast<uint16_t>(g_config.port));
    addr.sin_addr.s_addr = INADDR_ANY;
    if (bind(server_fd, reinterpret_cast<sockaddr *>(&addr), sizeof(addr)) < 0) {
        std::cerr << "bind failed: " << std::strerror(errno) << "\n";
        return 1;
    }
    if (listen(server_fd, 64) < 0) {
        std::cerr << "listen failed: " << std::strerror(errno) << "\n";
        return 1;
    }
    std::cout << "transcript ingestor cpp listening on http://" << g_config.host << ":" << g_config.port << std::endl;
    while (true) {
        int client = accept(server_fd, nullptr, nullptr);
        if (client < 0) continue;
        std::thread(handle_client, client).detach();
    }
}
