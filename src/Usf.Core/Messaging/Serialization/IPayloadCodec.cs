using System;

namespace Usf.Core.Messaging.Serialization;

/// <summary>
/// Encodes and decodes the data section of a CloudEvent.
/// </summary>
public interface IPayloadCodec
{
    EncodedPayload Encode<T>(T message);

    object? Decode(byte[] data, Type messageType);
}
