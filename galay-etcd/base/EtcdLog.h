#ifndef GALAY_ETCD_LOG_H
#define GALAY_ETCD_LOG_H

#include <memory>
#include <mutex>
#include <string>

#include <spdlog/spdlog.h>
#include <spdlog/sinks/basic_file_sink.h>
#include <spdlog/sinks/stdout_color_sinks.h>

namespace galay::etcd
{

using EtcdLoggerPtr = std::shared_ptr<spdlog::logger>;

class EtcdLog
{
public:
    static EtcdLog* getInstance()
    {
        static EtcdLog instance;
        return &instance;
    }

    static void enable()
    {
        console();
    }

    static void console()
    {
        console("EtcdLogger");
    }

    static void console(const std::string& logger_name)
    {
        auto instance = getInstance();
        std::lock_guard<std::mutex> lock(instance->m_mutex);
        try {
            auto logger = spdlog::get(logger_name);
            if (!logger) {
                logger = spdlog::stdout_color_mt(logger_name);
            }
            applyDefault(logger);
            instance->m_logger = std::move(logger);
        } catch (const spdlog::spdlog_ex&) {
            instance->m_logger = spdlog::get(logger_name);
        }
    }

    static void file(const std::string& log_file_path = "galay-etcd.log",
                     const std::string& logger_name = "EtcdLogger",
                     bool truncate = false)
    {
        auto instance = getInstance();
        std::lock_guard<std::mutex> lock(instance->m_mutex);
        auto logger = std::make_shared<spdlog::logger>(
            logger_name,
            std::make_shared<spdlog::sinks::basic_file_sink_mt>(log_file_path, !truncate));
        applyDefault(logger);
        instance->m_logger = std::move(logger);
    }

    static void disable()
    {
        auto instance = getInstance();
        std::lock_guard<std::mutex> lock(instance->m_mutex);
        if (instance->m_logger) {
            instance->m_logger->set_level(spdlog::level::off);
        }
        instance->m_logger.reset();
    }

    static void setLogger(EtcdLoggerPtr logger)
    {
        auto instance = getInstance();
        std::lock_guard<std::mutex> lock(instance->m_mutex);
        instance->m_logger = std::move(logger);
    }

    EtcdLoggerPtr getLogger() const
    {
        std::lock_guard<std::mutex> lock(m_mutex);
        return m_logger;
    }

private:
    static void applyDefault(const EtcdLoggerPtr& logger)
    {
        if (!logger) {
            return;
        }
        logger->set_pattern("[%Y-%m-%d %T.%e] [%^%L%$] [%s:%#] %v");
#ifdef ENABLE_DEBUG
        logger->set_level(spdlog::level::debug);
#else
        logger->set_level(spdlog::level::info);
#endif
    }

private:
    mutable std::mutex m_mutex;
    EtcdLoggerPtr m_logger;
};

} // namespace galay::etcd

#endif // GALAY_ETCD_LOG_H
