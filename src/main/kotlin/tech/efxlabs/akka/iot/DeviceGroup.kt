package tech.efxlabs.akka.iot

import akka.actor.typed.ActorRef
import akka.actor.typed.Behavior
import akka.actor.typed.PostStop
import akka.actor.typed.Signal
import akka.actor.typed.javadsl.AbstractBehavior
import akka.actor.typed.javadsl.ActorContext
import akka.actor.typed.javadsl.Behaviors
import akka.actor.typed.javadsl.Receive

class DeviceGroup private constructor(context: ActorContext<Command>, private val groupId: String) :
    AbstractBehavior<DeviceGroup.Command>(context) {

    private val deviceIdToActorRef = hashMapOf<String, ActorRef<Device.Command>>()

    init {
        context.log.info("Device Group Actor Initialized")
    }

    interface Command
    class DeviceTerminated(val actorRef: ActorRef<Device.Command>, val groupId: String, val deviceId: String) : Command


    override fun createReceive(): Receive<Command> {
        return newReceiveBuilder()
            .onMessage(DeviceManager.RequestTrackDevice::class.java, this::onTrackDevice)
            .onMessage(DeviceTerminated::class.java, this::onDeviceTermination)
            .onMessage(DeviceManager.RequestDeviceList::class.java,
                {it.groupId == groupId},
                this::onRequestDeviceList)
            .onSignal(PostStop::class.java, this::onStop)
            .build()
    }

    private fun onDeviceTermination(command: DeviceTerminated): Behavior<Command> {
        deviceIdToActorRef.remove(command.deviceId)
        context.log.info("Device [${command.deviceId}, ${command.groupId}] is terminated, size=[${deviceIdToActorRef.keys.size}]")
        return this
    }

    private fun onTrackDevice(command: DeviceManager.RequestTrackDevice): Behavior<Command> {
        if (command.groupId == groupId) {
            val deviceActor: ActorRef<Device.Command>? = deviceIdToActorRef[command.deviceId]
            if (deviceActor != null) {
                command.replyTo.tell(DeviceManager.DeviceRegistered(deviceActor))
            } else {
                context.log.info("Creating a device actor, ${command.deviceId}")
                val newDeviceActor =
                    context.spawn(Device.create(command.groupId, command.deviceId), "device-${command.deviceId}")
                context.watchWith(newDeviceActor, DeviceTerminated(newDeviceActor,groupId,command.deviceId))
                deviceIdToActorRef[command.deviceId] = newDeviceActor
                command.replyTo.tell(DeviceManager.DeviceRegistered(newDeviceActor))
            }
        } else {
            context.log.warn(
                "Ignoring TrackDevice request for {}. This actor is responsible for {}.",
                groupId,
                this.groupId
            )
        }

        return this
    }

    private fun onRequestDeviceList(command: DeviceManager.RequestDeviceList?): DeviceGroup {

        command?.replyTo?.tell(DeviceManager.ReplyDeviceList(command.requestId,deviceIdToActorRef.keys))
        return this
    }

    private fun onStop(signal: Signal): Behavior<Command> {
        context.log.info("DeviceGroup[ $groupId ] stopped!")
        return this
    }

    companion object {
        fun create(groupId: String): Behavior<Command> {
            return Behaviors.setup { DeviceGroup(it, groupId) }
        }
    }
}

/**
 * DeviceGroup is created by DeviceManager
 * Accepts Command -> RequestTrackDevice with caller actor ref reply to DeviceRegistered, which will have the actor reference to the device actor
 */

