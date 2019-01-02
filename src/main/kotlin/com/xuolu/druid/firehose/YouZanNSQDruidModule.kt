package com.xuolu.druid.firehose

import com.fasterxml.jackson.databind.Module
import com.fasterxml.jackson.databind.jsontype.NamedType
import com.fasterxml.jackson.databind.module.SimpleModule
import com.google.common.collect.ImmutableList
import com.google.inject.Binder
import org.apache.druid.initialization.DruidModule

class YouZanNSQDruidModule : DruidModule {
    override fun configure(binder: Binder) {}

    override fun getJacksonModules(): MutableList<out Module> {
        return ImmutableList.of(
            SimpleModule("YouZanNSQDruidModule").registerSubtypes(
                NamedType(YouZanNSQFirehoseFactory::class.java, "youzan_nsq")
            )
        )
    }
}