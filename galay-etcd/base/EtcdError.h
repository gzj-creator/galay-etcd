#ifndef GALAY_ETCD_ERROR_H
#define GALAY_ETCD_ERROR_H

#include <string>

namespace galay::etcd
{

enum EtcdErrorType
{
    ETCD_ERROR_SUCCESS = 0,
    ETCD_ERROR_INVALID_ENDPOINT,
    ETCD_ERROR_INVALID_PARAM,
    ETCD_ERROR_NOT_CONNECTED,
    ETCD_ERROR_CONNECTION,
    ETCD_ERROR_TIMEOUT,
    ETCD_ERROR_SEND,
    ETCD_ERROR_RECV,
    ETCD_ERROR_HTTP,
    ETCD_ERROR_SERVER,
    ETCD_ERROR_PARSE,
    ETCD_ERROR_INTERNAL,
};

class EtcdError
{
public:
    EtcdError(EtcdErrorType type = ETCD_ERROR_SUCCESS);
    EtcdError(EtcdErrorType type, std::string extra_msg);

    [[nodiscard]] EtcdErrorType type() const;
    [[nodiscard]] std::string message() const;
    [[nodiscard]] bool isOk() const;

private:
    EtcdErrorType m_type;
    std::string m_extra_msg;
};

} // namespace galay::etcd

#endif // GALAY_ETCD_ERROR_H
