// SPDX-License-Identifier: GPL-3.0-or-later
//
// squelch_uploader.cc — Trunk-Recorder Plugin_Api subclass for Squelch.
//
// Single translation unit, modelled on TR's bundled uploaders under
// `trunk-recorder/plugins/`. Everything lives here:
//
//   * `parse_config` (validation of server / apiKey / maxRetries plus the
//     `systems[]` array of { systemId, shortName, unitTagsFile? } entries)
//   * the libcurl multipart POST to `<server>/api/v1/calls`
//   * RFC 3339 / JSON / 50 MiB pre-flight projection of TR's `Call_Data_t`
//   * a single background upload thread with retry+backoff
//   * the `Plugin_Api` subclass and the `BOOST_DLL_ALIAS(create_plugin)`
//     factory TR's plugin loader looks for
//
// All non-public symbols sit in an anonymous namespace; only
// `squelch::SquelchUploader::create` crosses the ABI boundary (via
// `BOOST_DLL_ALIAS`).

#include "../../trunk-recorder/plugin_manager/plugin_api.h"

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

namespace squelch
{
    class SquelchUploader; // forward — defined below.
} // namespace squelch

namespace
{

    // ---------------------------------------------------------------------
    // Plugin identity
    // ---------------------------------------------------------------------

    constexpr const char *kPluginName = "squelch_uploader";
    constexpr const char *kPluginVersion = "0.2.2";

    // Squelch v1's upload-route ceiling. Files larger than this are rejected
    // before opening a connection.
    constexpr std::size_t kMaxUploadBytes = 50ULL * 1024ULL * 1024ULL;

    // ---------------------------------------------------------------------
    // Parsed plugin configuration
    //
    // Every plugin entry uses a `systems[]` array so a single instance can
    // fan out to one or more TR systems served by the same Squelch host:
    //
    //   {
    //     "name":       "squelch_uploader",
    //     "library":    "/usr/lib/trunk-recorder/plugins/squelch_uploader.so",
    //     "server":     "https://squelch.example.com",
    //     "apiKey":     "sk_live_…",
    //     "maxRetries": 3,
    //     "systems": [
    //       {
    //         "systemId": 1,
    //         "shortName": "MARCSLake",
    //         "unitTagsFile": "/etc/trunk-recorder/marcslake.csv"
    //       },
    //       { "systemId": 2, "shortName": "MARCSCuy" },
    //       { "systemId": 3, "shortName": "MARCSGea" }
    //     ]
    //   }
    //
    // Each completed call is routed to the matching entry by TR's
    // `call_info.short_name`; calls whose short_name is not listed are
    // dropped (logged at debug level) rather than uploaded. shortName
    // values must be unique within `systems[]`.
    // ---------------------------------------------------------------------

    struct SystemEntry
    {
        long system_id = 0;         // required, positive (JSON "systemId")
        std::string short_name;     // required + unique (JSON "shortName")
        std::string unit_tags_file; // optional (JSON "unitTagsFile")
    };

    struct PluginConfig
    {
        std::string server;  // required, http(s) URL
        std::string api_key; // required, non-empty (JSON "apiKey")

        // One entry per `systems[]` element. Always non-empty on success.
        std::vector<SystemEntry> systems;

        // Maximum retry attempts on transient failure (HTTP 408, 429, 5xx,
        // network errors). 0 disables retry; bounds 0..10.
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

    // Parses `data` into a PluginConfig. Returns std::nullopt and writes a
    // human-readable reason into *error on validation failure. *error is
    // untouched on success.
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

        // systems[] — required, non-empty array of routing entries. Top-
        // level systemId / shortName / unitTagsFile are not accepted; each
        // entry must carry its own.
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

                // shortName — required and used as the routing key, so it
                // must be non-empty and unique within the array.
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

    // ---------------------------------------------------------------------
    // TR-free projection of the per-call fields Squelch consumes.
    // ---------------------------------------------------------------------

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
        long start_time = 0; // epoch seconds (from TR's `start_time`)
        double freq = 0.0;
        double length = 0.0; // seconds
        long error_count = 0;
        long spike_count = 0;

        std::string talkgroup_tag;
        std::string talkgroup_alpha_tag;
        std::string talkgroup_description;
        std::string talkgroup_group;

        std::string audio_path;
        std::string short_name; // becomes `systemLabel`

        std::vector<CallSourceLite> transmission_source_list;
        std::vector<CallFreqLite> transmission_error_list;
        std::vector<unsigned long> patched_talkgroups;

        long system_id = 0;

        // Human-friendly identifier used in log lines. Never the API key.
        std::string debug_tag;
    };

    // ---------------------------------------------------------------------
    // Wire helpers
    // ---------------------------------------------------------------------

    constexpr std::chrono::milliseconds kBackoffBase{1000};
    constexpr std::chrono::milliseconds kBackoffCap{30000};
    constexpr double kJitterRatio = 0.20;

    // Mask all but the leading 6 chars of an API key. Never log the full secret.
    std::string redact(const std::string &key)
    {
        if (key.size() <= 6)
            return std::string(key.size(), '*');
        return key.substr(0, 6) + "…";
    }

    std::string to_lower(std::string s)
    {
        std::transform(s.begin(), s.end(), s.begin(),
                       [](unsigned char c)
                       { return std::tolower(c); });
        return s;
    }

    // Map an audio-file extension (case-insensitive) to its MIME type.
    // Trunk-Recorder only ever hands us a `.wav` (raw recording) or a
    // `.m4a` (when `compress_wav` is enabled), so those are the only
    // cases we recognise. Anything else falls back to a generic
    // `application/octet-stream` rather than guessing.
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

    // RFC 3339 UTC: `YYYY-MM-DDTHH:MM:SSZ`.
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

    // RFC 8259 §7 string escaping. `/` is left unescaped (legal either way).
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
            // freq rounded to integer Hz; pos/len keep two decimals.
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

    // The TR convention is that a single-entry list (the talkgroup itself)
    // means "not patched"; only emit the array when there's more than one.
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

    // First non-zero source id from transmission_source_list.
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

    // Look up the talker-alias tag attached to the source matching `unit_id`.
    // TR resolves unit_tags / OTA aliases into Call_Source.tag before
    // call_end fires, so we don't keep our own cache.
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

    // Append a text mime part. Skipped if `value` is empty and
    // `omit_when_empty` is true.
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

        // Per-thread RNG so jitter is non-deterministic without re-seeding
        // from random_device on every call.
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

    // Stat-based pre-flight. Returns true with `*size_out` populated if the
    // file exists and is in range; false with `*error` set otherwise
    // (missing, unreadable, too large).
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

    // ---------------------------------------------------------------------
    // Uploader — single background thread, unbounded FIFO, libcurl mime POST.
    // ---------------------------------------------------------------------

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
                // Never let an exception escape; TR is in our process.
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
                            long *status_out)
        {
            *error_out = {};
            *status_out = 0;

            if (curl_ == nullptr)
            {
                *error_out = "curl handle not initialised";
                return false;
            }

            // Reset prior options so a repeat call doesn't reuse stale state.
            curl_easy_reset(curl_);

            curl_mime *mime = curl_mime_init(curl_);

            // ---- required fields ----------------------------------------
            add_text_part(mime, "startedAt", to_rfc3339_utc(job.start_time),
                          /*omit_when_empty=*/false);
            add_text_part(mime, "systemId", std::to_string(job.system_id),
                          /*omit_when_empty=*/false);
            add_text_part(mime, "talkgroupId", std::to_string(job.talkgroup),
                          /*omit_when_empty=*/false);

            // ---- optional integers (omit when zero/unset) ---------------
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
            // sources / frequencies are emitted even when empty (so
            // operators can audit empty-array cases). patches is omitted
            // when the call wasn't patched.
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
            // Disable libcurl's automatic Expect: 100-continue.
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
                        return; // stopping with nothing left to do
                    job = std::move(queue_.front());
                    queue_.pop_front();
                }

                const unsigned attempts =
                    max_retries_ == 0 ? 1u : max_retries_ + 1u;

                for (unsigned attempt = 0; attempt < attempts; ++attempt)
                {
                    std::string err;
                    long status = 0;
                    bool ok = false;
                    try
                    {
                        ok = perform_upload(job, &err, &status);
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
                        if (attempt > 0)
                        {
                            BOOST_LOG_TRIVIAL(info)
                                << "[" << kPluginName
                                << "] upload succeeded after "
                                << (attempt + 1) << " attempts ("
                                << job.debug_tag << ")";
                        }
                        break;
                    }

                    const bool network_error = !err.empty();
                    const bool retriable = is_retriable(status, network_error);
                    const bool last_attempt = (attempt + 1 >= attempts);

                    if (!retriable)
                    {
                        BOOST_LOG_TRIVIAL(error)
                            << "[" << kPluginName << "] upload rejected (HTTP "
                            << status << ") for " << job.debug_tag
                            << "; not retrying";
                        break;
                    }

                    if (last_attempt)
                    {
                        if (network_error)
                        {
                            BOOST_LOG_TRIVIAL(error)
                                << "[" << kPluginName
                                << "] upload failed after " << attempts
                                << " attempts (network error: " << err
                                << ") for " << job.debug_tag;
                        }
                        else
                        {
                            BOOST_LOG_TRIVIAL(error)
                                << "[" << kPluginName
                                << "] upload failed after " << attempts
                                << " attempts (HTTP " << status << ") for "
                                << job.debug_tag;
                        }
                        break;
                    }

                    const auto delay = backoff_for_attempt(attempt);
                    BOOST_LOG_TRIVIAL(warning)
                        << "[" << kPluginName << "] upload attempt "
                        << (attempt + 1) << "/" << attempts << " failed ("
                        << (network_error ? err
                                          : "HTTP " + std::to_string(status))
                        << "); retrying in " << delay.count() << " ms ("
                        << job.debug_tag << ")";

                    // Sleep through the backoff unconditionally; stop()
                    // waits for the current retry chain plus the remaining
                    // queue to drain fully.
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

} // anonymous namespace

namespace squelch
{

    // -----------------------------------------------------------------------
    // Plugin_Api subclass — the only public symbol. Exported under the alias
    // `create_plugin` via BOOST_DLL_ALIAS at the bottom of this file.
    // -----------------------------------------------------------------------

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
                    << "[" << kPluginName << "] invalid config: " << error;
                return 1;
            }
            config_ = std::move(*parsed);
            std::ostringstream sys_summary;
            for (std::size_t i = 0; i < config_.systems.size(); ++i)
            {
                if (i != 0)
                    sys_summary << ", ";
                sys_summary << config_.systems[i].short_name
                            << "=" << config_.systems[i].system_id;
            }
            BOOST_LOG_TRIVIAL(info)
                << "[" << kPluginName << "] config parsed: server="
                << config_.server << " apiKey="
                << ::redact(config_.api_key) << " systems=["
                << sys_summary.str() << "]";
            return 0;
        }

        // NOTE: ::Config is TR's host-config struct; our PluginConfig lives
        // in the anonymous namespace above.
        int init(::Config *tr_config,
                 std::vector<Source *> sources,
                 std::vector<System *> systems) override
        {
            // Intentionally do NOT propagate tr_config->frequency_format into
            // TR's global `frequency_format` (defined in formatter.cc). That
            // symbol is exported by trunk_recorder_library, which the bundled
            // TR plugins link against directly. We build out-of-tree (headers
            // only via FetchContent), so referencing the global produces an
            // undefined symbol that dlopen() cannot resolve against the
            // visibility-hidden trunk-recorder executable:
            //
            //   dlerror: undefined symbol: frequency_format
            //
            // The plugin emits frequencyHz as a plain integer via its own
            // stringstreams, so it never needs TR's format_freq() helper and
            // therefore never needs the global at all.
            (void)tr_config;
            (void)sources;
            (void)systems;
            BOOST_LOG_TRIVIAL(info)
                << "[" << kPluginName << " " << kPluginVersion << "] init";
            return 0;
        }

        int start() override
        {
            BOOST_LOG_TRIVIAL(info) << "[" << kPluginName << "] start";

            if (config_.server.empty() || config_.api_key.empty())
            {
                // parse_config should have caught this, but defend anyway.
                BOOST_LOG_TRIVIAL(error)
                    << "[" << kPluginName
                    << "] start without server/apiKey; uploads disabled";
                return 1;
            }

            uploader_ = std::make_unique<::Uploader>(
                ::build_upload_url(config_.server),
                config_.api_key,
                config_.max_retries);
            uploader_->start();
            BOOST_LOG_TRIVIAL(info)
                << "[" << kPluginName << "] uploader started maxRetries="
                << config_.max_retries;
            return 0;
        }

        int stop() override
        {
            BOOST_LOG_TRIVIAL(info) << "[" << kPluginName << "] stop";
            if (uploader_)
            {
                uploader_->stop();
                uploader_.reset();
                BOOST_LOG_TRIVIAL(info)
                    << "[" << kPluginName << "] uploader stopped";
            }
            return 0;
        }

        // ----- per-call hooks -----------------------------------------------

        int call_start(Call *call) override
        {
            (void)call;
            return 0;
        }

        int call_end(Call_Data_t call_info) override
        {
            if (!uploader_)
            {
                BOOST_LOG_TRIVIAL(error)
                    << "[" << kPluginName
                    << "] uploader not running; dropping upload of "
                    << call_info.filename;
                return 1;
            }

            // Route to the matching SystemEntry by exact shortName. Calls
            // for systems not listed in `systems[]` are dropped at debug
            // level so a single plugin entry can fan out to a subset of
            // TR's systems.
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
                    << "[" << kPluginName
                    << "] dropping call for unconfigured system '"
                    << call_info.short_name << "' (tg="
                    << call_info.talkgroup << ")";
                return 0;
            }

            // Pick the on-disk audio path the way TR exposes it: use the
            // compressed file when call_info.compress_wav is set, otherwise
            // the raw `filename`.
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
            // shortName is required per entry, so always send the
            // configured value as `systemLabel`.
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
                lite.length = 0.0; // TR's Call_Source has no per-tx length;
                                   // duration_ms covers the whole call.
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
                lite.freq = call_info.freq; // per-call freq; TR's Call_Error
                                            // doesn't carry one.
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
                    << "[" << kPluginName << "] " << preflight_error;
                return 1;
            }

            job.debug_tag = "tg=" + std::to_string(job.talkgroup) +
                            " started=" + std::to_string(job.start_time) +
                            " bytes=" + std::to_string(audio_size);

            uploader_->enqueue(std::move(job));
            return 0;
        }

        // ----- recorder / system / source setup -----------------------------

        int setup_recorder(Recorder *recorder) override
        {
            (void)recorder;
            return 0;
        }

        int setup_system(System *system) override
        {
            (void)system;
            return 0;
        }

        int setup_systems(std::vector<System *> systems) override
        {
            (void)systems;
            return 0;
        }

        int setup_sources(std::vector<Source *> sources) override
        {
            (void)sources;
            return 0;
        }

        // ----- unit-level hooks ---------------------------------------------

        int unit_registration(System *sys, long source_id) override
        {
            (void)sys;
            (void)source_id;
            return 0;
        }

        int unit_deregistration(System *sys, long source_id) override
        {
            (void)sys;
            (void)source_id;
            return 0;
        }

        int unit_acknowledge_response(System *sys, long source_id) override
        {
            (void)sys;
            (void)source_id;
            return 0;
        }

        int unit_data_grant(System *sys, long source_id) override
        {
            (void)sys;
            (void)source_id;
            return 0;
        }

        int unit_answer_request(System *sys,
                                long source_id,
                                long talkgroup) override
        {
            (void)sys;
            (void)source_id;
            (void)talkgroup;
            return 0;
        }

        int unit_location(System *sys,
                          long source_id,
                          long talkgroup_num) override
        {
            (void)sys;
            (void)source_id;
            (void)talkgroup_num;
            return 0;
        }

        // ----- factory ------------------------------------------------------

        static boost::shared_ptr<SquelchUploader> create()
        {
            return boost::shared_ptr<SquelchUploader>(new SquelchUploader());
        }

    private:
        ::PluginConfig config_{};
        std::unique_ptr<::Uploader> uploader_;
    };

} // namespace squelch

// Exported under the alias name `create_plugin`, which is what TR's plugin
// loader looks for.
BOOST_DLL_ALIAS(
    squelch::SquelchUploader::create, // <-- this function is exported with…
    create_plugin                     // <-- …this alias name
)
