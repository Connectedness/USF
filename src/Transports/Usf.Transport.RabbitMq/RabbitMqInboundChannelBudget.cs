using System;
using System.Collections.Generic;
using System.Linq;

namespace Usf.Transport.RabbitMq;

public static class RabbitMqInboundChannelBudget
{
    public static (int WorstCaseChannelCount, string Description) Calculate(
        IReadOnlyList<RabbitMqInboundChannelGroup> channelGroups
    )
    {
        if (channelGroups is null)
        {
            throw new ArgumentNullException(nameof(channelGroups));
        }

        if (channelGroups.Count == 0)
        {
            return (0, "no channel groups configured");
        }

        var worstCaseChannelCount = channelGroups.Sum(static group => group.MaximumChannelCount);

        if (channelGroups.Count == 1)
        {
            var channelGroup = channelGroups[0];
            return (
                worstCaseChannelCount,
                $"channel group '{channelGroup.Name}' max {channelGroup.MaximumChannelCount}"
            );
        }

        return (worstCaseChannelCount, $"{channelGroups.Count} channel groups");
    }
}
