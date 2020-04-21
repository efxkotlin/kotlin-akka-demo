package tech.efxlabs.akka.tests

import akka.actor.testkit.typed.javadsl.TestKitJunitResource
import akka.actor.typed.ActorRef
import org.junit.Assert.assertEquals
import org.junit.Assert.assertNotEquals
import org.junit.ClassRule
import org.junit.Test
import tech.efxlabs.akka.iot.Device
import tech.efxlabs.akka.iot.DeviceGroup
import tech.efxlabs.akka.iot.DeviceManager
import java.util.*

class DeviceTest {

    companion object {
        @JvmField
        @ClassRule
        val testKit:TestKitJunitResource = TestKitJunitResource()
    }

    @Test
    fun replyWithEmptyTemperatureIfNonePresent() {
        val testProbe = testKit.createTestProbe(Device.Command.RespondTemperature::class.java)

        val deviceActor = testKit.spawn(Device.create("group","device"))
        deviceActor.tell(Device.Command.ReadTemperature(42L,testProbe.ref))

        val response = testProbe.receiveMessage()

        assertEquals(42L, response.requestId)
        assertEquals(Optional.empty<String>(), response.temperatureValue)
    }

    @Test
    fun testReplyWithLatestReading() {
        val recordTemperatureProbe = testKit.createTestProbe(Device.Command.TemperatureRecorded::class.java)
        val respondTemperatureProbe = testKit.createTestProbe(Device.Command.RespondTemperature::class.java)

        val deviceActor = testKit.spawn(Device.create("group","device"))
        deviceActor.tell(Device.Command.RecordTemperature(24L,22.0,recordTemperatureProbe.ref))

        assertEquals(24L,recordTemperatureProbe.receiveMessage().requestId)

        deviceActor.tell(Device.Command.ReadTemperature(24L,respondTemperatureProbe.ref))
        val receiveMessage1 = respondTemperatureProbe.receiveMessage()
        assertEquals(22.0,receiveMessage1.temperatureValue.get(),0.0)
        assertEquals(24L,receiveMessage1.requestId)

        deviceActor.tell(Device.Command.RecordTemperature(24L,33.0,recordTemperatureProbe.ref))
        assertEquals(24L,recordTemperatureProbe.receiveMessage().requestId)

        deviceActor.tell(Device.Command.ReadTemperature(24L,respondTemperatureProbe.ref))
        val receiveMessage2 = respondTemperatureProbe.receiveMessage()
        assertEquals(33.0,receiveMessage2.temperatureValue.get(),0.0)
        assertEquals(24L,receiveMessage2.requestId)
    }

    @Test
    fun testDeviceRegistrationAndReplies() {
        val probe = testKit.createTestProbe(DeviceManager.DeviceRegistered::class.java)

        val deviceGroupActor:ActorRef<DeviceGroup.Command> = testKit.spawn(DeviceGroup.create("group"))
        deviceGroupActor.tell(DeviceManager.RequestTrackDevice(24L,"group","11ADF",probe.ref))
        val deviceReg1 = probe.receiveMessage()
        deviceGroupActor.tell(DeviceManager.RequestTrackDevice(25L,"group","10ABE",probe.ref))
        val deviceReg2 = probe.receiveMessage()

        val device1 = deviceReg1.actorRef
        val device2 = deviceReg2.actorRef
        assertNotEquals(device1,device2)

        val deviceProbe1 = testKit.createTestProbe(Device.Command.TemperatureRecorded::class.java)
        device1.tell(Device.Command.RecordTemperature(24L,22.0,deviceProbe1.ref))
        assertEquals(deviceProbe1.receiveMessage().requestId, 24L)

        device2.tell(Device.Command.RecordTemperature(25L,27.0,deviceProbe1.ref))
        assertEquals(deviceProbe1.receiveMessage().requestId, 25L)
    }

    @Test
    fun testRegisterDeviceIdempotence() {

        val probe = testKit.createTestProbe(DeviceManager.DeviceRegistered::class.java)

        val deviceGroup1 = testKit.spawn(DeviceGroup.create("group"))
        deviceGroup1.tell(DeviceManager.RequestTrackDevice(22L,"group", "11WWE",probe.ref))
        val probeMessage1 = probe.receiveMessage()
        deviceGroup1.tell(DeviceManager.RequestTrackDevice(22L,"group", "11WWE",probe.ref))
        val probeMessage2 = probe.receiveMessage()
        assertEquals(probeMessage1.actorRef, probeMessage2.actorRef)
    }

    @Test
    fun testActiveListAfterOneShutDown() {
        val probe = testKit.createTestProbe(DeviceManager.DeviceRegistered::class.java)

        val deviceGroupActor:ActorRef<DeviceGroup.Command> = testKit.spawn(DeviceGroup.create("group"))
        deviceGroupActor.tell(DeviceManager.RequestTrackDevice(24L,"group","11ADF",probe.ref))
        val deviceReg1 = probe.receiveMessage()
        deviceGroupActor.tell(DeviceManager.RequestTrackDevice(25L,"group","10ABE",probe.ref))
        val deviceReg2 = probe.receiveMessage()

        val device1 = deviceReg1.actorRef
        val device2 = deviceReg2.actorRef
        assertNotEquals(device1,device2)

        val deviceProbe1 = testKit.createTestProbe(Device.Command.TemperatureRecorded::class.java)
        device1.tell(Device.Command.RecordTemperature(24L,22.0,deviceProbe1.ref))
        assertEquals(deviceProbe1.receiveMessage().requestId, 24L)

        device2.tell(Device.Command.RecordTemperature(25L,27.0,deviceProbe1.ref))
        assertEquals(deviceProbe1.receiveMessage().requestId, 25L)

        device2.tell(Device.Command.Passivate(25L))

        val replyActiveListProbe = testKit.createTestProbe(DeviceManager.ReplyDeviceList::class.java)
        deviceGroupActor.tell(DeviceManager.RequestDeviceList(25L,"group",replyActiveListProbe.ref))

        replyActiveListProbe.expectTerminated(device2,replyActiveListProbe.remainingOrDefault)

        replyActiveListProbe.awaitAssert {
            deviceGroupActor.tell(DeviceManager.RequestDeviceList(25L,"group",replyActiveListProbe.ref))
            val ids = replyActiveListProbe.receiveMessage().ids
            assertEquals(1,ids.size)
            assertEquals(ids, setOf("11ADF"))
            return@awaitAssert null
        }
    }

    @Test
    fun testFlowFromDeviceManager() {
        val testProbe = testKit.createTestProbe(DeviceManager.DeviceRegistered::class.java)
        val deviceManagerActor = testKit.spawn(DeviceManager.create())
        deviceManagerActor.tell(DeviceManager.RequestTrackDevice(1L, "group1","11SWQ",testProbe.ref))
        val deviceActor1 = testProbe.receiveMessage().actorRef

        val testProbeReplyDevices = testKit.createTestProbe(DeviceManager.ReplyDeviceList::class.java)
        deviceManagerActor.tell(DeviceManager.RequestDeviceList(1L,"group1",testProbeReplyDevices.ref))
        val devicesList = testProbeReplyDevices.receiveMessage()
        assertEquals(devicesList.ids.size, 1)

        deviceActor1.tell(Device.Command.Passivate(1L))

        testProbeReplyDevices.expectTerminated(deviceActor1,testProbeReplyDevices.remainingOrDefault)

        testProbeReplyDevices.awaitAssert {
            deviceManagerActor.tell(DeviceManager.RequestDeviceList(25L,"group",testProbeReplyDevices.ref))
            val ids = testProbeReplyDevices.receiveMessage().ids
            assertEquals(0,ids.size)
            assertEquals(ids, setOf<String>())
            return@awaitAssert null
        }

    }
}