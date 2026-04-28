// SPDX-License-Identifier: GPL-3.0-or-later
//
// squelch_uploader.cc — TR plugin that uploads completed calls to
// Squelch over HTTPS. Single translation unit modeled on TR's
// bundled uploaders. Contains:
//
//   * config parsing (server / apiKey / maxRetries / systems[])
//   * libcurl multipart POST to <server>/api/v1/calls
//   * 50 MiB pre-flight check on the audio file
//   * background upload thread with retry + backoff
//   * Plugin_Api subclass and BOOST_DLL_ALIAS export
//
// Internals live in an anonymous namespace; only `create_plugin`
// crosses the ABI boundary.

#include "../../trunk-recorder/plugin_manager/plugin_api.h"
#include "../../trunk-recorder/formatter.h"

#include <curl/curl.h>

#include <boost/dll/alias.hpp>
#include <boost/log/trivial.hpp>
#include <boost/shared_ptr.hpp>

#include <json.hpp>

#include <sys/stat.h>

#include <algorithm>
#include <cctype>
#include <chrono>
#include <cmath>
#include <condition_variable>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <ctime>
#include <deque>
#include <memory>
#include <mutex>
#include <optional>
#include <random>
#include <sstream>
#include <string>
#include <thread>
#include <utility>
#include <vector>

namespace
{

    constexpr const char *kPluginName = "squelch_uploader";
    constexpr const char *kPluginVersion = "0.2.2";
    constexpr const char *kLogPrefix = "\t[Squelch]\t";
    constexpr std::size_t kMaxUploadBytes = 50ULL * 1024ULL * 1024ULL;

    // Plugin config. One instance can fan out to multiple TR systems on
    // the same Squelch host. Calls are routed by matching shortName;
    // calls for unlisted systems are dropped.
    //
    //   {
    //     "server":     "https://squelch.example.com",
    //     "apiKey":     "sk_live_…",
    //     "maxRetries": 3,
    //     "systems": [
    //       { "systemId": 1, "shortName": "MARCSLake" },
    //       { "systemId": 2, "shortName": "MARCSCuy" }
    //     ]
    //   }

    struct SystemEntry
    {
        long system_id = 0;         // required, positive
        std::string short_name;     // required, unique
        std::string unit_tags_file; // optional
    };

    struct PluginConfig
    {
        std::string server;  // required, http(s) URL
        std::string api_key; // required, non-empty
        std::vector<SystemEntry> systems;

        // Retry attempts on transient failure (HTTP 408, 429, 5xx,
        // network errors). 0 disables retry. Range: 0..10.
        unsigned max_retries = 3;
    };

    bool starts_with(const std::string &s, const char *prefix)
    {
        const std::size_t n = std::char_traits<char>::length(prefix);
        return s.size() >= n && s.compare(0, n, prefix) == 0;
    }

    template <typename T>
    bool get_optional_string(const nlohmann::json &data, const char *key, T &out)
    {
        auto it = data.find(key);
        if (it == data.end() || it->is_null())
            return false;
        out = it->get<T>();
        return true;
    }

    // Parse `data` into a PluginConfig. Returns nullopt with *error set
    // on validation failure. *error is untouched on success.
    std::optional<PluginConfig> parse_plugin_config(const nlohmann::json &data,
                                                    std::string *error)
    {
        if (!data.is_object())
        {
            if (error)
                *error = "config must be a JSON object";
            return std::nullopt;
        }

        PluginConfig cfg;

        // server — required, http(s) URL.
        {
            auto it = data.find("server");
            if (it == data.end() || !it->is_string())
            {
                if (error)
                    *error = "missing required string field: server";
                return std::nullopt;
            }
            cfg.server = it->get<std::string>();
            if (cfg.server.empty())
            {
                if (error)
                    *error = "server must not be empty";
                return std::nullopt;
            }
            if (!starts_with(cfg.server, "https://") &&
                !starts_with(cfg.server, "http://"))
            {
                if (error)
                {
                    *error = "server must start with https:// or http:// (got '" +
                             cfg.server + "')";
                }
                return std::nullopt;
            }
        }

        // apiKey — required, non-empty.
        {
            auto it = data.find("apiKey");
            if (it == data.end() || !it->is_string())
            {
                if (error)
                    *error = "missing required string field: apiKey";
                return std::nullopt;
            }
            cfg.api_key = it->get<std::string>();
            if (cfg.api_key.empty())
            {
                if (error)
                    *error = "apiKey must not be empty";
                return std::nullopt;
            }
        }

        // systems[] — required, non-empty array. Top-level systemId /
        // shortName / unitTagsFile are not accepted.
        {
            const auto systems_it = data.find("systems");
            if (systems_it == data.end() || systems_it->is_null())
            {
                if (error)
                    *error = "missing required field: systems";
                return std::nullopt;
            }
            if (!systems_it->is_array())
            {
                if (error)
                    *error = "systems must be a JSON array";
                return std::nullopt;
            }
            if (systems_it->empty())
            {
                if (error)
                    *error = "systems must not be empty";
                return std::nullopt;
            }

            for (const char *legacy : {"systemId", "shortName", "unitTagsFile"})
            {
                if (data.contains(legacy) && !data.at(legacy).is_null())
                {
                    if (error)
                    {
                        *error = std::string("top-level '") + legacy +
                                 "' is not supported; move it into a "
                                 "systems[] entry";
                    }
                    return std::nullopt;
                }
            }

            cfg.systems.reserve(systems_it->size());

            for (std::size_t i = 0; i < systems_it->size(); ++i)
            {
                const auto &el = (*systems_it)[i];
                const std::string idx = "systems[" + std::to_string(i) + "]";
                if (!el.is_object())
                {
                    if (error)
                        *error = idx + " must be an object";
                    return std::nullopt;
                }

                SystemEntry entry;

                // systemId — required, positive integer.
                {
                    auto sid = el.find("systemId");
                    if (sid == el.end() || sid->is_null())
                    {
                        if (error)
                            *error = idx + " missing required field: systemId";
                        return std::nullopt;
                    }
                    if (!sid->is_number_integer())
                    {
                        if (error)
                            *error = idx + ".systemId must be an integer";
                        return std::nullopt;
                    }
                    entry.system_id = sid->get<long>();
                    if (entry.system_id <= 0)
                    {
                        if (error)
                        {
                            *error = idx +
                                     ".systemId must be a positive integer";
                        }
                        return std::nullopt;
                    }
                }

                // shortName — required, non-empty, unique in systems[].
                {
                    auto sn = el.find("shortName");
                    if (sn == el.end() || !sn->is_string())
                    {
                        if (error)
                        {
                            *error = idx +
                                     " missing required string field: "
                                     "shortName";
                        }
                        return std::nullopt;
                    }
                    entry.short_name = sn->get<std::string>();
                    if (entry.short_name.empty())
                    {
                        if (error)
                            *error = idx + ".shortName must not be empty";
                        return std::nullopt;
                    }
                    for (const auto &prior : cfg.systems)
                    {
                        if (prior.short_name == entry.short_name)
                        {
                            if (error)
                            {
                                *error =
                                    "duplicate shortName '" +
                                    entry.short_name + "' in systems[]";
                            }
                            return std::nullopt;
                        }
                    }
                }

                // unitTagsFile — optional.
                get_optional_string<std::string>(el, "unitTagsFile",
                                                 entry.unit_tags_file);

                cfg.systems.push_back(std::move(entry));
            }
        }

        // maxRetries — optional, range 0..10.
        {
            auto it = data.find("maxRetries");
            if (it != data.end() && !it->is_null())
            {
                if (!it->is_number_integer() && !it->is_number_unsigned())
                {
                    if (error)
                        *error = "maxRetries must be an integer";
                    return std::nullopt;
                }
                const auto raw = it->get<std::int64_t>();
                if (raw < 0 || raw > 10)
                {
                    if (error)
                    {
                        *error = "maxRetries must be between 0 and 10 (got " +
                                 std::to_string(raw) + ")";
                    }
                    return std::nullopt;
                }
                cfg.max_retries = static_cast<unsigned>(raw);
            }
        }

        return cfg;
    }

    struct CallSourceLite
    {
        long source = 0;
        long time = 0;
        double position = 0.0;
        double length = 0.0;
        bool emergency = false;
        std::string signal_system;
        std::string tag;
    };

    struct CallFreqLite
    {
        double freq = 0.0;
        long time = 0;
        double position = 0.0;
        double total_len = 0.0;
        long error_count = 0;
        long spike_count = 0;
    };

    struct UploadJob
    {
        long talkgroup = 0;
        long start_time = 0;
        double freq = 0.0;
        double length = 0.0;
        long error_count = 0;
        long spike_count = 0;

        std::string talkgroup_tag;
        std::string talkgroup_alpha_tag;
        std::string talkgroup_description;
        std::string talkgroup_group;

        std::string audio_path;
        std::string short_name;

        std::vector<CallSourceLite> transmission_source_list;
        std::vector<CallFreqLite> transmission_error_list;
        std::vector<unsigned long> patched_talkgroups;

        long system_id = 0;

        std::uintmax_t audio_bytes = 0;

        // Pre-rendered TR log_header() prefix:
        //   "[short]\t<call>C\tTG: <tg>\tFreq: <freq>\t"
        std::string log_prefix;
    };

    constexpr std::chrono::milliseconds kBackoffBase{1000};
    constexpr std::chrono::milliseconds kBackoffCap{30000};
    constexpr double kJitterRatio = 0.20;

    std::string redact(const std::string &key)
    {
        if (key.size() <= 2)
            return std::string(8, '*');
        return std::string(6, '*') + key.substr(key.size() - 2);
    }

    std::string to_lower(std::string s)
    {
        std::transform(s.begin(), s.end(), s.begin(),
                       [](unsigned char c)
                       { return std::tolower(c); });
        return s;
    }

    std::string audio_content_type_for(const std::string &path)
    {
        const auto dot = path.find_last_of('.');
        if (dot == std::string::npos || dot + 1 >= path.size())
            return "application/octet-stream";
        const std::string ext = to_lower(path.substr(dot + 1));
        if (ext == "wav")
            return "audio/wav";
        if (ext == "m4a")
            return "audio/m4a";
        return "application/octet-stream";
    }

    // RFC 3339 UTC timestamp: "YYYY-MM-DDTHH:MM:SSZ".
    std::string to_rfc3339_utc(std::time_t epoch_s)
    {
        std::tm tm_utc{};
        if (gmtime_r(&epoch_s, &tm_utc) == nullptr)
            return std::string{};
        char buf[32];
        const std::size_t n =
            std::strftime(buf, sizeof(buf), "%FT%TZ", &tm_utc);
        if (n == 0)
            return std::string{};
        return std::string(buf, n);
    }

    // RFC 8259 string escaping.
    void json_escape_into(const std::string &s, std::ostringstream &out)
    {
        out.put('"');
        for (char ch : s)
        {
            const auto c = static_cast<unsigned char>(ch);
            switch (c)
            {
            case '"':
                out << "\\\"";
                break;
            case '\\':
                out << "\\\\";
                break;
            case '\b':
                out << "\\b";
                break;
            case '\f':
                out << "\\f";
                break;
            case '\n':
                out << "\\n";
                break;
            case '\r':
                out << "\\r";
                break;
            case '\t':
                out << "\\t";
                break;
            default:
                if (c < 0x20)
                {
                    char tmp[8];
                    std::snprintf(tmp, sizeof(tmp), "\\u%04x", c);
                    out << tmp;
                }
                else
                {
                    out.put(ch);
                }
                break;
            }
        }
        out.put('"');
    }

    std::string fmt_fixed(double v, int precision)
    {
        char buf[64];
        std::snprintf(buf, sizeof(buf), "%.*f", precision, v);
        return std::string(buf);
    }

    std::string build_sources_json(const std::vector<CallSourceLite> &items)
    {
        if (items.empty())
            return "[]";
        std::ostringstream out;
        out << '[';
        for (std::size_t i = 0; i < items.size(); ++i)
        {
            const auto &s = items[i];
            if (i != 0)
                out << ',';
            out << "{\"src\":" << s.source
                << ",\"time\":" << s.time
                << ",\"pos\":" << fmt_fixed(s.position, 2)
                << ",\"len\":" << fmt_fixed(s.length, 2)
                << ",\"emergency\":" << (s.emergency ? "true" : "false")
                << ",\"signal_system\":";
            json_escape_into(s.signal_system, out);
            out << ",\"tag\":";
            json_escape_into(s.tag, out);
            out << '}';
        }
        out << ']';
        return out.str();
    }

    std::string build_frequencies_json(const std::vector<CallFreqLite> &items)
    {
        if (items.empty())
            return "[]";
        std::ostringstream out;
        out << '[';
        for (std::size_t i = 0; i < items.size(); ++i)
        {
            const auto &f = items[i];
            if (i != 0)
                out << ',';
            // freq as integer Hz; pos/len keep two decimals.
            const std::int64_t freq_hz =
                static_cast<std::int64_t>(std::llround(f.freq));
            out << "{\"freq\":" << freq_hz
                << ",\"time\":" << f.time
                << ",\"pos\":" << fmt_fixed(f.position, 2)
                << ",\"len\":" << fmt_fixed(f.total_len, 2)
                << ",\"errorCount\":" << f.error_count
                << ",\"spikeCount\":" << f.spike_count
                << '}';
        }
        out << ']';
        return out.str();
    }

    // TR convention: a single-entry list means "not patched".
    std::string build_patches_json(const std::vector<unsigned long> &items)
    {
        if (items.size() <= 1)
            return std::string{};
        std::ostringstream out;
        out << '[';
        for (std::size_t i = 0; i < items.size(); ++i)
        {
            if (i != 0)
                out << ',';
            out << items[i];
        }
        out << ']';
        return out.str();
    }

    // First non-zero source id from the source list.
    std::optional<std::int64_t>
    first_unit_id(const std::vector<CallSourceLite> &sources)
    {
        for (const auto &s : sources)
        {
            if (s.source > 0)
                return static_cast<std::int64_t>(s.source);
        }
        return std::nullopt;
    }

    // TR resolves unit_tags / OTA aliases into Call_Source.tag before
    // call_end fires.
    std::string alias_for_unit(const std::vector<CallSourceLite> &sources,
                               std::optional<std::int64_t> unit_id)
    {
        if (!unit_id)
            return {};
        for (const auto &s : sources)
        {
            if (static_cast<std::int64_t>(s.source) == *unit_id &&
                !s.tag.empty())
                return s.tag;
        }
        return {};
    }

    // Append a text mime part. Skipped when value is empty unless
    // omit_when_empty is false.
    void add_text_part(curl_mime *mime, const char *name,
                       const std::string &value,
                       bool omit_when_empty = true)
    {
        if (omit_when_empty && value.empty())
            return;
        curl_mimepart *part = curl_mime_addpart(mime);
        curl_mime_name(part, name);
        curl_mime_data(part, value.c_str(), value.size());
    }

    void ensure_curl_global_init()
    {
        static std::once_flag once;
        std::call_once(once, []()
                       { curl_global_init(CURL_GLOBAL_DEFAULT); });
    }

    std::size_t write_cb(void *contents, std::size_t size, std::size_t nmemb,
                         void *userp)
    {
        const std::size_t bytes = size * nmemb;
        auto *buf = static_cast<std::string *>(userp);
        buf->append(static_cast<char *>(contents), bytes);
        return bytes;
    }

    bool is_retriable(long status_code, bool network_error) noexcept
    {
        if (network_error)
            return true;
        if (status_code >= 500 && status_code < 600)
            return true;
        if (status_code == 408 || status_code == 429)
            return true;
        return false;
    }

    // Read the Squelch error envelope when present:
    //   {"error":{"code":"duplicate_call","message":"..."}}
    // Otherwise fall back to a short description of the HTTP status.
    std::string describe_failure(long status, const std::string &body)
    {
        std::string code, message;
        try
        {
            auto j = nlohmann::json::parse(body, nullptr, false);
            if (j.is_object() && j.contains("error") &&
                j["error"].is_object())
            {
                const auto &e = j["error"];
                if (e.contains("code") && e["code"].is_string())
                    code = e["code"].get<std::string>();
                if (e.contains("message") && e["message"].is_string())
                    message = e["message"].get<std::string>();
            }
        }
        catch (...)
        {
            // Not JSON; use status-code defaults.
        }

        std::ostringstream out;
        out << "HTTP " << status;
        switch (status)
        {
        case 400:
            out << " bad request";
            break;
        case 401:
            out << " unauthorized (check apiKey)";
            break;
        case 403:
            out << " forbidden (apiKey lacks access to system)";
            break;
        case 404:
            out << " not found (check server URL)";
            break;
        case 409:
            out << " duplicate call (already accepted)";
            break;
        case 413:
            out << " payload too large";
            break;
        case 415:
            out << " unsupported audio format";
            break;
        case 422:
            out << " call rejected by validation";
            break;
        case 429:
            out << " rate limited";
            break;
        default:
            if (status >= 500 && status < 600)
                out << " server error";
            break;
        }
        if (!code.empty())
            out << " [" << code << "]";
        if (!message.empty())
            out << ": " << message;
        return out.str();
    }

    std::chrono::milliseconds backoff_for_attempt(unsigned attempt_index)
    {
        std::int64_t delay = kBackoffBase.count();
        const unsigned safe_shift = std::min<unsigned>(attempt_index, 30u);
        for (unsigned i = 0; i < safe_shift; ++i)
        {
            if (delay >= kBackoffCap.count())
            {
                delay = kBackoffCap.count();
                break;
            }
            delay *= 2;
        }
        if (delay > kBackoffCap.count())
            delay = kBackoffCap.count();

        static thread_local std::mt19937_64 rng(std::random_device{}());
        std::uniform_real_distribution<double> dist(0.0, 1.0);
        const double r = dist(rng);
        const double scale = 1.0 + kJitterRatio * (2.0 * r - 1.0);
        const double scaled = static_cast<double>(delay) * scale;
        if (scaled <= 0.0)
            return std::chrono::milliseconds{0};
        return std::chrono::milliseconds{static_cast<std::int64_t>(scaled)};
    }

    std::string build_upload_url(const std::string &server)
    {
        std::string base = server;
        while (!base.empty() && base.back() == '/')
            base.pop_back();
        return base + "/api/v1/calls";
    }

    // Verify the audio file exists, is regular, and fits the size cap.
    bool check_audio_file(const std::string &path,
                          std::uintmax_t *size_out,
                          std::string *error)
    {
        if (path.empty())
        {
            if (error)
                *error = "audio path is empty";
            return false;
        }
        struct stat st{};
        if (::stat(path.c_str(), &st) != 0)
        {
            if (error)
                *error = "audio file not found: " + path;
            return false;
        }
        if (!S_ISREG(st.st_mode))
        {
            if (error)
                *error = "audio path is not a regular file: " + path;
            return false;
        }
        const auto size = static_cast<std::uintmax_t>(st.st_size);
        if (size > kMaxUploadBytes)
        {
            if (error)
            {
                *error = "audio file exceeds 50 MiB limit (" +
                         std::to_string(size) + " bytes): " + path;
            }
            return false;
        }
        if (size_out)
            *size_out = size;
        return true;
    }

    // Uploader — single background thread, FIFO queue, libcurl mime POST.
    class Uploader
    {
    public:
        Uploader(std::string url, std::string bearer_token,
                 unsigned max_retries)
            : url_(std::move(url)),
              bearer_token_(std::move(bearer_token)),
              max_retries_(max_retries)
        {
            ensure_curl_global_init();
            curl_ = curl_easy_init();
        }

        ~Uploader()
        {
            try
            {
                stop();
            }
            catch (...)
            {
                // Don't let exceptions escape; we share TR's process.
            }
            if (curl_ != nullptr)
            {
                curl_easy_cleanup(curl_);
                curl_ = nullptr;
            }
        }

        Uploader(const Uploader &) = delete;
        Uploader &operator=(const Uploader &) = delete;

        void start()
        {
            std::scoped_lock lk(mu_);
            if (started_)
                return;
            started_ = true;
            stopping_ = false;
            worker_ = std::thread([this]()
                                  { worker_loop(); });
        }

        void enqueue(UploadJob job)
        {
            {
                std::scoped_lock lk(mu_);
                queue_.push_back(std::move(job));
            }
            cv_.notify_one();
        }

        void stop()
        {
            {
                std::scoped_lock lk(mu_);
                if (!started_)
                    return;
                stopping_ = true;
            }
            cv_.notify_all();
            if (worker_.joinable())
                worker_.join();
            std::scoped_lock lk(mu_);
            started_ = false;
        }

    private:
        bool perform_upload(const UploadJob &job, std::string *error_out,
                            long *status_out, std::string *body_out)
        {
            *error_out = {};
            *status_out = 0;
            *body_out = {};

            if (curl_ == nullptr)
            {
                *error_out = "curl handle not initialised";
                return false;
            }

            curl_easy_reset(curl_);

            curl_mime *mime = curl_mime_init(curl_);

            // ---- required fields ----------------------------------------
            add_text_part(mime, "startedAt", to_rfc3339_utc(job.start_time),
                          /*omit_when_empty=*/false);
            add_text_part(mime, "systemId", std::to_string(job.system_id),
                          /*omit_when_empty=*/false);
            add_text_part(mime, "talkgroupId", std::to_string(job.talkgroup),
                          /*omit_when_empty=*/false);

            // ---- optional integers (omitted when zero/unset) ------------
            if (job.freq > 0.0)
            {
                const auto freq_hz =
                    static_cast<std::int64_t>(std::llround(job.freq));
                add_text_part(mime, "frequencyHz", std::to_string(freq_hz));
            }
            if (job.length > 0.0)
            {
                const auto duration_ms =
                    static_cast<std::int64_t>(std::llround(job.length * 1000.0));
                add_text_part(mime, "durationMs", std::to_string(duration_ms));
            }
            const auto unit_id = first_unit_id(job.transmission_source_list);
            if (unit_id)
                add_text_part(mime, "unitId", std::to_string(*unit_id));
            if (job.error_count > 0)
                add_text_part(mime, "errorCount",
                              std::to_string(job.error_count));
            if (job.spike_count > 0)
                add_text_part(mime, "spikeCount",
                              std::to_string(job.spike_count));

            // ---- JSON-string fields -------------------------------------
            // sources / frequencies always sent (even when empty);
            // patches omitted when the call wasn't patched.
            add_text_part(mime, "sources",
                          build_sources_json(job.transmission_source_list),
                          /*omit_when_empty=*/false);
            add_text_part(mime, "frequencies",
                          build_frequencies_json(job.transmission_error_list),
                          /*omit_when_empty=*/false);
            add_text_part(mime, "patches",
                          build_patches_json(job.patched_talkgroups));

            // ---- optional plain strings ---------------------------------
            add_text_part(mime, "talkgroupLabel", job.talkgroup_alpha_tag);
            add_text_part(mime, "talkgroupTag", job.talkgroup_tag);
            add_text_part(mime, "talkgroupGroup", job.talkgroup_group);
            add_text_part(mime, "talkgroupName", job.talkgroup_description);
            add_text_part(mime, "talkerAlias",
                          alias_for_unit(job.transmission_source_list, unit_id));
            add_text_part(mime, "systemLabel", job.short_name);

            // ---- audio file part ----------------------------------------
            {
                curl_mimepart *part = curl_mime_addpart(mime);
                curl_mime_name(part, "audio");
                curl_mime_filedata(part, job.audio_path.c_str());
                const std::string ctype = audio_content_type_for(job.audio_path);
                curl_mime_type(part, ctype.c_str());
            }

            // ---- headers / options --------------------------------------
            curl_slist *headerlist = nullptr;
            const std::string auth = "Authorization: Bearer " + bearer_token_;
            headerlist = curl_slist_append(headerlist, auth.c_str());
            headerlist = curl_slist_append(headerlist, "Expect:");
            const std::string user_agent =
                std::string(kPluginName) + "/" + kPluginVersion;

            char errbuf[CURL_ERROR_SIZE];
            errbuf[0] = '\0';
            std::string response_body;

            curl_easy_setopt(curl_, CURLOPT_URL, url_.c_str());
            curl_easy_setopt(curl_, CURLOPT_USERAGENT, user_agent.c_str());
            curl_easy_setopt(curl_, CURLOPT_HTTPHEADER, headerlist);
            curl_easy_setopt(curl_, CURLOPT_MIMEPOST, mime);
            curl_easy_setopt(curl_, CURLOPT_WRITEFUNCTION, write_cb);
            curl_easy_setopt(curl_, CURLOPT_WRITEDATA, &response_body);
            curl_easy_setopt(curl_, CURLOPT_NOSIGNAL, 1L);
            curl_easy_setopt(curl_, CURLOPT_FOLLOWLOCATION, 0L);
            curl_easy_setopt(curl_, CURLOPT_SSL_VERIFYPEER, 1L);
            curl_easy_setopt(curl_, CURLOPT_SSL_VERIFYHOST, 2L);
            curl_easy_setopt(curl_, CURLOPT_CONNECTTIMEOUT, 10L);
            curl_easy_setopt(curl_, CURLOPT_TIMEOUT, 60L);
            curl_easy_setopt(curl_, CURLOPT_ERRORBUFFER, errbuf);

            const CURLcode rc = curl_easy_perform(curl_);
            bool network_error = false;
            if (rc != CURLE_OK)
            {
                *error_out = errbuf[0] != '\0'
                                 ? std::string(errbuf)
                                 : std::string(curl_easy_strerror(rc));
                network_error = true;
            }
            else
            {
                long status = 0;
                curl_easy_getinfo(curl_, CURLINFO_RESPONSE_CODE, &status);
                *status_out = status;
            }

            curl_slist_free_all(headerlist);
            curl_mime_free(mime);

            *body_out = std::move(response_body);

            if (network_error)
                return false;
            return *status_out >= 200 && *status_out < 300;
        }

        void worker_loop()
        {
            for (;;)
            {
                UploadJob job;
                {
                    std::unique_lock lk(mu_);
                    cv_.wait(lk, [this]
                             { return stopping_ || !queue_.empty(); });
                    if (queue_.empty())
                        return;
                    job = std::move(queue_.front());
                    queue_.pop_front();
                }

                const unsigned attempts =
                    max_retries_ == 0 ? 1u : max_retries_ + 1u;

                for (unsigned attempt = 0; attempt < attempts; ++attempt)
                {
                    std::string err;
                    long status = 0;
                    std::string body;
                    bool ok = false;
                    try
                    {
                        ok = perform_upload(job, &err, &status, &body);
                    }
                    catch (const std::exception &ex)
                    {
                        err = std::string("uploader exception: ") + ex.what();
                        status = 0;
                    }
                    catch (...)
                    {
                        err = "uploader unknown exception";
                        status = 0;
                    }

                    if (ok)
                    {
                        BOOST_LOG_TRIVIAL(info)
                            << job.log_prefix
                            << "Squelch Upload Success - file size: "
                            << job.audio_bytes;
                        if (attempt > 0)
                        {
                            BOOST_LOG_TRIVIAL(info)
                                << job.log_prefix
                                << "Squelch Upload succeeded after "
                                << (attempt + 1) << " attempts";
                        }
                        break;
                    }

                    const bool network_error = !err.empty();
                    const bool retriable = is_retriable(status, network_error);
                    const bool last_attempt = (attempt + 1 >= attempts);
                    const std::string reason =
                        network_error ? err : describe_failure(status, body);

                    if (!retriable)
                    {
                        if (status == 409)
                        {
                            BOOST_LOG_TRIVIAL(info)
                                << job.log_prefix
                                << "Squelch Upload skipped: " << reason;
                        }
                        else
                        {
                            BOOST_LOG_TRIVIAL(error)
                                << job.log_prefix
                                << "Squelch Upload rejected: " << reason
                                << "; not retrying";
                        }
                        break;
                    }

                    if (last_attempt)
                    {
                        BOOST_LOG_TRIVIAL(error)
                            << job.log_prefix
                            << "Squelch Upload failed after " << attempts
                            << " attempts ("
                            << (network_error ? "network error: " : "")
                            << reason << ")";
                        break;
                    }

                    const auto delay = backoff_for_attempt(attempt);
                    BOOST_LOG_TRIVIAL(warning)
                        << job.log_prefix
                        << "Squelch Upload attempt "
                        << (attempt + 1) << "/" << attempts << " failed ("
                        << reason
                        << "); retrying in " << delay.count() << " ms";

                    std::this_thread::sleep_for(delay);
                }
            }
        }

        const std::string url_;
        const std::string bearer_token_;
        const unsigned max_retries_;

        CURL *curl_ = nullptr;

        mutable std::mutex mu_;
        std::condition_variable cv_;
        std::deque<UploadJob> queue_;
        bool started_ = false;
        bool stopping_ = false;

        std::thread worker_;
    };

}

namespace squelch
{

    class SquelchUploader : public Plugin_Api
    {
    public:
        SquelchUploader() = default;
        ~SquelchUploader() override = default;

        int parse_config(json config_data) override
        {
            std::string error;
            auto parsed = ::parse_plugin_config(config_data, &error);
            if (!parsed)
            {
                BOOST_LOG_TRIVIAL(error)
                    << kLogPrefix << "invalid config: " << error;
                return 1;
            }
            config_ = std::move(*parsed);

            BOOST_LOG_TRIVIAL(info)
                << kLogPrefix << "Squelch Server: " << config_.server
                << "\t API Key: " << ::redact(config_.api_key);
            for (const auto &sys : config_.systems)
            {
                BOOST_LOG_TRIVIAL(info)
                    << kLogPrefix << "Uploading calls for: " << sys.short_name
                    << "\t Squelch System: " << sys.system_id;
            }
            if (config_.systems.empty())
            {
                BOOST_LOG_TRIVIAL(error)
                    << kLogPrefix
                    << "Squelch Server set, but no Systems are configured";
            }
            return 0;
        }

        int start() override
        {
            if (config_.server.empty() || config_.api_key.empty())
            {
                BOOST_LOG_TRIVIAL(error)
                    << kLogPrefix
                    << "start without server/apiKey; uploads disabled";
                return 1;
            }

            uploader_ = std::make_unique<::Uploader>(
                ::build_upload_url(config_.server),
                config_.api_key,
                config_.max_retries);
            uploader_->start();
            return 0;
        }

        int stop() override
        {
            if (uploader_)
            {
                uploader_->stop();
                uploader_.reset();
            }
            return 0;
        }

        int call_end(Call_Data_t call_info) override
        {
            if (!uploader_)
            {
                BOOST_LOG_TRIVIAL(error)
                    << ::log_header(call_info.short_name,
                                    call_info.call_num,
                                    call_info.talkgroup_display,
                                    call_info.freq)
                    << "Squelch uploader not running; dropping upload of "
                    << call_info.filename;
                return 1;
            }

            // Route by exact shortName; unlisted systems are dropped
            // at debug level so one plugin instance can serve a subset
            // of TR's systems.
            const ::SystemEntry *entry = nullptr;
            for (const auto &s : config_.systems)
            {
                if (s.short_name == call_info.short_name)
                {
                    entry = &s;
                    break;
                }
            }
            if (entry == nullptr)
            {
                BOOST_LOG_TRIVIAL(debug)
                    << ::log_header(call_info.short_name,
                                    call_info.call_num,
                                    call_info.talkgroup_display,
                                    call_info.freq)
                    << "Squelch dropping call for unconfigured system";
                return 0;
            }

            // Use the compressed (m4a) file when compress_wav is on.
            ::UploadJob job;
            job.talkgroup = call_info.talkgroup;
            job.start_time = call_info.start_time;
            job.freq = call_info.freq;
            job.length = call_info.length;
            job.error_count = call_info.error_count;
            job.spike_count = call_info.spike_count;
            job.talkgroup_tag = call_info.talkgroup_tag;
            job.talkgroup_alpha_tag = call_info.talkgroup_alpha_tag;
            job.talkgroup_description = call_info.talkgroup_description;
            job.talkgroup_group = call_info.talkgroup_group;
            job.audio_path = call_info.compress_wav ? call_info.converted
                                                    : call_info.filename;
            job.short_name = entry->short_name;
            job.patched_talkgroups = std::vector<unsigned long>(
                call_info.patched_talkgroups.begin(),
                call_info.patched_talkgroups.end());
            job.system_id = entry->system_id;

            job.transmission_source_list.reserve(
                call_info.transmission_source_list.size());
            for (const auto &s : call_info.transmission_source_list)
            {
                ::CallSourceLite lite;
                lite.source = s.source;
                lite.time = s.time;
                lite.position = s.position;
                lite.length = 0.0; // Call_Source has no per-tx length.
                lite.emergency = s.emergency;
                lite.signal_system = s.signal_system;
                lite.tag = s.tag;
                job.transmission_source_list.push_back(std::move(lite));
            }

            job.transmission_error_list.reserve(
                call_info.transmission_error_list.size());
            for (const auto &f : call_info.transmission_error_list)
            {
                ::CallFreqLite lite;
                lite.freq = call_info.freq;
                lite.time = f.time;
                lite.position = f.position;
                lite.total_len = f.total_len;
                lite.error_count = static_cast<long>(f.error_count);
                lite.spike_count = static_cast<long>(f.spike_count);
                job.transmission_error_list.push_back(std::move(lite));
            }

            // Pre-flight: file must exist and be ≤ 50 MiB.
            std::uintmax_t audio_size = 0;
            std::string preflight_error;
            if (!::check_audio_file(job.audio_path, &audio_size,
                                    &preflight_error))
            {
                BOOST_LOG_TRIVIAL(error)
                    << ::log_header(call_info.short_name,
                                    call_info.call_num,
                                    call_info.talkgroup_display,
                                    call_info.freq)
                    << "Squelch Upload preflight failed: "
                    << preflight_error;
                return 1;
            }

            job.audio_bytes = audio_size;

            job.log_prefix = ::log_header(call_info.short_name,
                                          call_info.call_num,
                                          call_info.talkgroup_display,
                                          call_info.freq);

            uploader_->enqueue(std::move(job));
            return 0;
        }

        static boost::shared_ptr<SquelchUploader> create()
        {
            return boost::shared_ptr<SquelchUploader>(new SquelchUploader());
        }

    private:
        ::PluginConfig config_{};
        std::unique_ptr<::Uploader> uploader_;
    };

}

// Exported under the alias `create_plugin`, which TR's plugin loader dlsym()s.
BOOST_DLL_ALIAS(
    squelch::SquelchUploader::create,
    create_plugin
)
