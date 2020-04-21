package tech.efxlabs.akka.iot

import akka.actor.typed.Behavior
import akka.actor.typed.PostStop
import akka.actor.typed.Signal
import akka.actor.typed.javadsl.AbstractBehavior
import akka.actor.typed.javadsl.ActorContext
import akka.actor.typed.javadsl.Behaviors
import akka.actor.typed.javadsl.Receive

class IoTSupervisor private constructor(context: ActorContext<Void>):AbstractBehavior<Void>(context) {

    init {
        context.log.info("IoT-Supervisor Started")
    }

    override fun createReceive(): Receive<Void> {
        return newReceiveBuilder()
            .onSignal(PostStop::class.java,this::onStop)
            .build()
    }

    private fun onStop(signal: Signal): Behavior<Void> {
        context.log.info("IoT-Supervisor Stopped")
        return this
    }

    companion object {
        fun create():Behavior<Void> {
            return Behaviors.setup(::IoTSupervisor)
        }
    }

}
