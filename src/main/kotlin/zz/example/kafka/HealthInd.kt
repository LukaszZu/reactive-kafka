package zz.example.kafka

import org.springframework.boot.actuate.health.Health
import org.springframework.boot.actuate.health.HealthIndicator
import org.springframework.boot.actuate.health.Status
import org.springframework.cloud.stream.messaging.DirectWithAttributesChannel
import org.springframework.context.ApplicationContext
import org.springframework.integration.channel.FluxMessageChannel
import org.springframework.integration.channel.PublishSubscribeChannel
import org.springframework.messaging.MessageChannel
import org.springframework.messaging.SubscribableChannel
import org.springframework.stereotype.Component

@Component
class HealthInd(val channles: ApplicationContext) : HealthIndicator {

    override fun health(): Health {
        val channelStatuses = channles.getBeanDefinitionNames().map {
            val b = channles.getBean(it)
            when (b) {
                is DirectWithAttributesChannel ->
                    if (b.subscriberCount == 0) Pair(Health.down(), b) else Pair(Health.up(), b)
                else -> null
            }
        }.filterNotNull()

        val hout = Health.unknown()

        channelStatuses.map { hout.withDetail(it.second.beanName, it.second.subscriberCount) }
        val bad = channelStatuses
            .filter { it.first.build().status != Status.UP }
            .map { it.first.build().status }
            .firstOrNull() ?: Status.UP

        return hout.status(bad).build()
    }

}