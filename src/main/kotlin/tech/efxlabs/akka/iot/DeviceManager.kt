package tech.efxlabs.akka.iot

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.PostStop
import akka.actor.typed.Signal
import akka.actor.typed.javadsl.AbstractBehavior
import akka.actor.typed.javadsl.ActorContext
import akka.actor.typed.javadsl.Behaviors
import akka.actor.typed.javadsl.Receive

class DeviceManager private constructor(context: ActorContext<Command>) :
    AbstractBehavior<DeviceManager.Command>(context) {

    init {
        context.log.info("Device Manager Initialized!")
    }

    private val groupIdToActorMap = hashMapOf<String,ActorRef<DeviceGroup.Command>>()

    interface Command
    class RequestTrackDevice(val requestId:Long, val groupId:String, val deviceId:String, val replyTo:ActorRef<DeviceRegistered>)
        : DeviceManager.Command,DeviceGroup.Command
    class DeviceRegistered(val actorRef: ActorRef<Device.Command>)
    class RequestDeviceList(val requestId: Long, val groupId: String, val replyTo: ActorRef<ReplyDeviceList>): Command, DeviceGroup.Command
    class ReplyDeviceList(val requestId: Long, val ids:Set<String>)
    class DeviceGroupTerminated(val groupId: String):Command

    override fun createReceive(): Receive<Command> {
        return newReceiveBuilder()
            .onSignal(PostStop::class.java, this::onStop)
            .onMessage(DeviceGroupTerminated::class.java, this::onDeviceGroupTerminated)
            .onMessage(RequestTrackDevice::class.java, this::requestTrackDevice)
            .onMessage(RequestDeviceList::class.java, this::requestDeviceList)
            .build()
    }

    private fun requestTrackDevice(command:RequestTrackDevice):Behavior<Command> {
        if (groupIdToActorMap.containsKey(command.groupId)) {
            groupIdToActorMap[command.groupId]!!.tell(command)
        } else {
            context.log.info("Creating Device Group for ${command.groupId}")
            val groupActor = context.spawn(DeviceGroup.create(command.groupId), "DeviceGroup-${command.groupId}")
            context.watchWith(groupActor,DeviceGroupTerminated(command.groupId))
            groupActor.tell(command)
            groupIdToActorMap[command.groupId] = groupActor
        }
        return this
    }

    private fun requestDeviceList(command:RequestDeviceList):Behavior<Command> {
        if (groupIdToActorMap.containsKey(command.groupId)) {
            groupIdToActorMap[command.groupId]!!.tell(command)
        } else {
            command.replyTo.tell(ReplyDeviceList(command.requestId,setOf()))
        }
        return this
    }

    private fun onStop(signal: Signal):Behavior<Command> {
        context.log.info("Manager Actor Stopped. Bye")
        return this
    }

    private fun onDeviceGroupTerminated(command:DeviceGroupTerminated):Behavior<Command> {
        context.log.info("Device Group has been terminated, DeviceGroup[${command.groupId}]")
        groupIdToActorMap.remove(command.groupId)
        return this
    }

    companion object {
        fun create(): Behavior<Command> {
            return Behaviors.setup { DeviceManager(it) }
        }
    }
}