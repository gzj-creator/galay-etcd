#ifndef GALAY_ETCD_ERROR_H
#define GALAY_ETCD_ERROR_H

#include <string>

namespace galay::etcd
{

enum class EtcdErrorType
{
    Success = 0,
    InvalidEndpoint,
    InvalidParam,
    NotConnected,
    Connection,
    Timeout,
    Send,
    Recv,
    Http,
    Server,
    Parse,
    Internal,
};

class EtcdError
{
public:
    EtcdError(EtcdErrorType type = EtcdErrorType::Success);
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
