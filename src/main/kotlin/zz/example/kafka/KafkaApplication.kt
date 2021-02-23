package zz.example.kafka

import org.apache.kafka.clients.consumer.KafkaConsumer
import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.autoconfigure.kafka.ConcurrentKafkaListenerContainerFactoryConfigurer
import org.springframework.boot.runApplication
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.kafka.listener.*
import org.springframework.kafka.support.Acknowledgment
import org.springframework.kafka.support.KafkaHeaders
import org.springframework.messaging.Message
import reactor.core.publisher.Flux
import reactor.core.publisher.Hooks
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.lang.RuntimeException
import java.time.Duration
import java.util.function.Consumer
import java.util.function.Function
import java.util.concurrent.TimeUnit
import java.util.concurrent.atomic.AtomicLong


@SpringBootApplication
class KafkaApplication

fun main(args: Array<String>) {
    runApplication<KafkaApplication>(*args)
}

data class SomeData(val id: String, val data: String)

@Configuration
class KafkaConsumerApp {

    //processing one by one
    @Bean
    fun myConsumer2() = Function<Flux<Message<SomeData>>, Mono<Void>> { stream ->
        stream
            .doOnNext { println("Start processing ${Thread.currentThread().name}") }
            .concatMap({ batch ->
                Mono.just(batch)
                    .doOnNext { println("Processing ${Thread.currentThread().name}") }
                    .flatMap {
                        println("--> In processing ${Thread.currentThread().name}")
                        Mono.just(it).delayElement(Duration.ofSeconds(2))
                    }.doOnSuccess {
                        println("Doing commit ${Thread.currentThread().name}")
                    }
            }, 0)
            .doOnNext { println("Processed ${Thread.currentThread().name}") }
            .then()
    }


    //processing in parallel
    @Bean
    fun myConsumer3() = Function<Flux<Message<SomeData>>, Mono<Void>> { stream ->
        stream
            .index()
            .doOnRequest { println("Requested $it") }
            .flatMapSequential ({ batch ->
                Mono.just(batch)
                    .doOnNext { println("Processing ${batch.t1} -> ${Thread.currentThread().name}") }
                    .flatMap {
                        Mono.just(it).delayElement(Duration.ofSeconds(5))
                    }
            }, 1)
            .doOnNext {
                println("Processed ${it.t1} -> ${Thread.currentThread().name}")
                (it.t2.headers[KafkaHeaders.ACKNOWLEDGMENT] as Acknowledgment).acknowledge()
            }
            .then()
    }


    //just for explosion
    @Bean
    fun myConsumer() = Function<Flux<Message<SomeData>>, Mono<Void>> { stream ->
        stream
            .map { throw RuntimeException("Ups boom") }
            .doOnCancel {
                println("Canceled--------------------------------------")
            }
            .then()
    }
}