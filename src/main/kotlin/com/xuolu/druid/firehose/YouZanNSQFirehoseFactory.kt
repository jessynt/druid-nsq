package com.xuolu.druid.firehose

import com.fasterxml.jackson.annotation.JsonCreator
import com.fasterxml.jackson.annotation.JsonProperty
import com.google.common.base.Preconditions
import com.youzan.nsq.client.ConsumerImplV2
import com.youzan.nsq.client.MessageHandler
import com.youzan.nsq.client.entity.NSQConfig
import com.youzan.nsq.client.entity.NSQMessage
import org.apache.druid.data.input.Committer
import org.apache.druid.data.input.FirehoseFactoryV2
import org.apache.druid.data.input.FirehoseV2
import org.apache.druid.data.input.InputRow
import org.apache.druid.data.input.impl.InputRowParser
import org.apache.druid.java.util.common.logger.Logger
import org.apache.druid.java.util.common.parsers.ParseException
import java.nio.ByteBuffer
import java.util.*
import java.util.concurrent.LinkedBlockingQueue

class YouZanNSQFirehoseFactory @JsonCreator constructor(
    @JsonProperty("lookupd_http_addresses") private val lookupdHttpAddresses: String,
    @JsonProperty("enable_ordered") private val enableOrdered: Boolean,
    @JsonProperty("queueBufferLength") private var queueBufferLength: Int,
    @JsonProperty("topic") private val topic: String,
    @JsonProperty("channel") private val channel: String
) : FirehoseFactoryV2<InputRowParser<ByteBuffer>> {

    private val log = Logger(YouZanNSQFirehoseFactory::class.java)

    init {
        Preconditions.checkArgument(!lookupdHttpAddresses.isEmpty(), "lookupdHttpAddresses is null/empty")
        if (queueBufferLength <= 0) this.queueBufferLength = 20000
        Preconditions.checkArgument(!topic.isEmpty(), "topic is null/empty")
        Preconditions.checkArgument(!channel.isEmpty(), "channel is null/empty")
    }

    override fun connect(parser: InputRowParser<ByteBuffer>, lastCommit: Any): FirehoseV2 {
        val messageQueue = LinkedBlockingQueue<NSQMessage>()

        val nsqConfig = NSQConfig("BaseConsumer")
        nsqConfig.setLookupAddresses(this.lookupdHttpAddresses)
        nsqConfig.isOrdered = this.enableOrdered

        val consumer = ConsumerImplV2(nsqConfig)
        val handler = MessageHandler(messageQueue::put)

        consumer.run {
            setAutoFinish(false)
            subscribe(topic)
            setMessageHandler(handler)
        }

        return object : FirehoseV2 {
            @Volatile
            private var closed: Boolean = false

            @Volatile
            private var row: InputRow? = null

            @Volatile
            private var nextIterator = Collections.emptyIterator<InputRow>()

            override fun start() = consumer.start()

            override fun makeCommitter(): Committer {
                return object : Committer {
                    override fun run() {}
                    override fun getMetadata(): Any {
                        return true
                    }
                }
            }

            override fun advance(): Boolean {
                if (closed) return false
                nextMessage()
                return true
            }

            private fun nextMessage() = try {
                row = null
                while (row == null) {
                    if (!nextIterator.hasNext()) {
                        val message: NSQMessage = messageQueue.take()
                        message.finish()
                        nextIterator = parser.parseBatch(ByteBuffer.wrap(message.messageBody)).iterator()
                        continue
                    }
                    row = nextIterator.next()
                }
            } catch (e: InterruptedException) {
                // Let the caller decide whether to stop or continue when thread is interrupted.
                log.warn(e, "Thread Interrupted while taking from queue, propagating the interrupt")
                Thread.currentThread().interrupt()
            }

            override fun currRow(): InputRow? {
                if (closed) return null

                if (row == null) try {
                    nextMessage()
                } catch (e: ParseException) {
                    return null
                }

                return row
            }

            override fun close() {
                log.info("Closing connection to NSQ")
                closed = true
                messageQueue.clear()
                consumer.close()
            }
        }
    }
}