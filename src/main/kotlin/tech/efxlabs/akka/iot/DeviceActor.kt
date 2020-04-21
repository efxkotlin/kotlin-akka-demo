package tech.efxlabs.akka.iot

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.PostStop
import akka.actor.typed.Signal
import akka.actor.typed.javadsl.*
import java.util.*

class Device private constructor(context:ActorContext<Command>, private val groupId:String, private val deviceId:String)
    : AbstractBehavior<Device.Command>(context){

    private var lastTemperatureReading: Optional<Double> = Optional.empty()

    init {
        context.log.info("Device[ $deviceId, $groupId ] started!")
    }

    sealed class Command {
        class ReadTemperature(val requestId: Long, val replyTo: ActorRef<RespondTemperature>) : Command()
        class RespondTemperature(val requestId: Long, val temperatureValue: Optional<Double>)
        class RecordTemperature(val requestId: Long, val temperatureValue:Double, val replyTo:ActorRef<TemperatureRecorded>) : Command()
        class TemperatureRecorded(val requestId: Long)
        class Passivate(val requestId: Long):Command()
    }


    override fun createReceive(): Receive<Command> {
        return newReceiveBuilder()
            .onMessage(Command.ReadTemperature::class.java, this::readTemperature)
            .onMessage(Command.RecordTemperature::class.java, this::recordTemperature)
            .onMessage(Command.Passivate::class.java){Behaviors.stopped()}
            .onSignal(PostStop::class.java, this::onStop)
            .build()
    }

    private fun readTemperature(command: Command.ReadTemperature):Behavior<Command> {
        command.replyTo.tell(Command.RespondTemperature(command.requestId,lastTemperatureReading))
        return this
    }

    private fun recordTemperature(command: Command.RecordTemperature): Behavior<Command> {
        context.log.info("Temperature recorded with value: ${command.temperatureValue}, requestId: ${command.requestId} by Device [$deviceId, $groupId]")
        lastTemperatureReading = Optional.of(command.temperatureValue)
        command.replyTo.tell(Command.TemperatureRecorded(command.requestId))
        return this
    }

    private fun onStop(signal: Signal):Behavior<Command> {
        context.log.info("Device[ $deviceId, $groupId ] stopped!")
        return this
    }

    companion object {
        fun create(groupId: String, deviceId: String): Behavior<Command> {
            return Behaviors.setup {
                context -> Device(context,groupId, deviceId)}
        }
    }

}

/**
 * Device Actors respond to ReadTemperature and RecordTemperature Commands
 * ReadTemperature will have a reply to actor reference of RespondTemperature.
 * RecordTemperature will have a reply to actor reference of type TemperatureRecorded
 */
