package com.ociweb.opendds.subvalve;

import DDS.*;
import Nexmatix.*;
import OpenDDS.DCPS.TheParticipantFactory;
import OpenDDS.DCPS.TheServiceParticipant;
import org.omg.CORBA.StringSeqHolder;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Enumeration;
import java.util.HashMap;
import java.util.Map;
import java.util.jar.JarEntry;
import java.util.jar.JarFile;

public class SubValve {
    // place native opendds dynamic libs in project current working directory
    static {
        try {
            final String osName = System.getProperty("os.name").toLowerCase();
            String nativeJarName = null;
            if (osName.contains("mac")) {
                nativeJarName = "OpenDDSDarwin.jar";
            } else if (osName.contains("linux")) {
                nativeJarName = "OpenDDSLinux.jar";
            } else if (osName.contains("Windows")) {
                nativeJarName = "OpenDDSWindows.jar";
            }

            if (nativeJarName == null) {
                throw new UnsupportedOperationException("No known OpenDDS native jar for OS " + osName);
            } else {
                System.out.println(nativeJarName + " contains native libaries for " + osName);
            }

            final String currentWorkingDirString = Paths.get("").toAbsolutePath().normalize().toString();
            final Path jarFilePath = Paths.get(currentWorkingDirString, nativeJarName);
            Files.deleteIfExists(jarFilePath);
            final InputStream stream = Thread.currentThread().getContextClassLoader().getResourceAsStream(nativeJarName);
            Files.copy(stream, jarFilePath);
            stream.close();

            if (Files.exists(jarFilePath)) {
                // unpack Jar to cwd
                Map<String, String> libFileNameMap = new HashMap<>();
                JarFile jar = new JarFile(jarFilePath.toString());
                for (Enumeration<JarEntry> enumEntries = jar.entries(); enumEntries.hasMoreElements(); ) {
                    final JarEntry entry = enumEntries.nextElement();
                    final int startIndex = 0;
                    final int endIndex = entry.getName().indexOf(".");
                    final String key = entry.getName().substring(startIndex, endIndex).toLowerCase();
                    final Path dynamicLibPath = Paths.get(currentWorkingDirString, entry.getName());
                    libFileNameMap.put(key, entry.getName());
                    Files.deleteIfExists(dynamicLibPath);
                    Files.copy(jar.getInputStream(entry), dynamicLibPath);
                }
                jar.close();

                //libFileNameMap.forEach((id, val) -> System.out.println(id + ":" + val));

                // load dynamic libraries with path to current directory to
                // support rpath location mechanism to resolve to unpacked library path
                final String libs[] = {
                        "libACE",
                        "libTAO",
                        "libTAO_AnyTypeCode",
                        "libTAO_PortableServer",
                        "libTAO_CodecFactory",
                        "libTAO_PI",
                        "libTAO_BiDirGIOP",
                        "libidl2jni_runtime",
                        "libtao_java",
                        "libOpenDDS_Dcps",
                        "libOpenDDS_Udp",
                        "libOpenDDS_Tcp",
                        "libOpenDDS_Rtps",
                        "libOpenDDS_Rtps_Udp",
                        "libOpenDDS_DCPS_Java"
                };
                for (String lib : libs) {
                    final String key = lib.toLowerCase();
                    if (libFileNameMap.containsKey(key)) {
                        final String libFileName = libFileNameMap.get(key);
                        System.out.println("Loading: " + libFileName);
                        System.load(Paths.get(currentWorkingDirString, libFileName).toAbsolutePath().normalize().toString());
                    } else {
                        System.out.println("Skipping: " + lib);
                    }
                }
            }

        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static final int VALVE_PARTICIPANT = 23;
    private static final String VALVE_TOPIC = "Valve";


    private static boolean checkReliable(String[] paramArrayOfString) {
        for (String aParamArrayOfString : paramArrayOfString) {
            if (aParamArrayOfString.equals("-r")) {
                return true;
            }
        }
        return false;
    }

    public static void main(String[] args)
            throws Exception {
        System.out.println("Start Valve Subscriber");
        boolean reliable = checkReliable(args);

        DomainParticipantFactory localDomainParticipantFactory = TheParticipantFactory.WithArgs(new StringSeqHolder(args));
        if (localDomainParticipantFactory == null) {
            System.err.println("ERROR: Domain Participant Factory not found");
            return;
        }

        DomainParticipant localDomainParticipant = localDomainParticipantFactory.create_participant(VALVE_PARTICIPANT, PARTICIPANT_QOS_DEFAULT.get(), null, -1);
        if (localDomainParticipant == null) {
            System.err.println("ERROR: Domain Participant creation failed");
            return;
        }
        ValveDataTypeSupportImpl valveDataTypeSupport = new ValveDataTypeSupportImpl();
        if (valveDataTypeSupport.register_type(localDomainParticipant, "") != 0) {
            System.err.println("ERROR: register_type failed");
            return;
        }

        Topic localTopic = localDomainParticipant.create_topic(
                VALVE_TOPIC,
                valveDataTypeSupport.get_type_name(),
                TOPIC_QOS_DEFAULT.get(),
                null,
                -1);

        if (localTopic == null) {
            System.err.println("ERROR: Topic creation failed");
            return;
        }

        Subscriber localSubscriber = localDomainParticipant.create_subscriber(
                SUBSCRIBER_QOS_DEFAULT.get(),
                null,
                -1);
        if (localSubscriber == null) {
            System.err.println("ERROR: Subscriber creation failed");
            return;
        }

        DataReaderQos localDataReaderQos = new DataReaderQos();
        localDataReaderQos.durability = new DurabilityQosPolicy();
        localDataReaderQos.durability.kind = DurabilityQosPolicyKind.from_int(0);
        localDataReaderQos.deadline = new DeadlineQosPolicy();
        localDataReaderQos.deadline.period = new Duration_t();
        localDataReaderQos.latency_budget = new LatencyBudgetQosPolicy();
        localDataReaderQos.latency_budget.duration = new Duration_t();
        localDataReaderQos.liveliness = new LivelinessQosPolicy();
        localDataReaderQos.liveliness.kind = LivelinessQosPolicyKind.from_int(0);
        localDataReaderQos.liveliness.lease_duration = new Duration_t();
        localDataReaderQos.reliability = new ReliabilityQosPolicy();
        localDataReaderQos.reliability.kind = ReliabilityQosPolicyKind.from_int(0);
        localDataReaderQos.reliability.max_blocking_time = new Duration_t();
        localDataReaderQos.destination_order = new DestinationOrderQosPolicy();
        localDataReaderQos.destination_order.kind = DestinationOrderQosPolicyKind.from_int(0);
        localDataReaderQos.history = new HistoryQosPolicy();
        localDataReaderQos.history.kind = HistoryQosPolicyKind.from_int(0);
        localDataReaderQos.resource_limits = new ResourceLimitsQosPolicy();
        localDataReaderQos.user_data = new UserDataQosPolicy();
        localDataReaderQos.user_data.value = new byte[0];
        localDataReaderQos.ownership = new OwnershipQosPolicy();
        localDataReaderQos.ownership.kind = OwnershipQosPolicyKind.from_int(0);
        localDataReaderQos.time_based_filter = new TimeBasedFilterQosPolicy();
        localDataReaderQos.time_based_filter.minimum_separation = new Duration_t();
        localDataReaderQos.reader_data_lifecycle = new ReaderDataLifecycleQosPolicy();
        localDataReaderQos.reader_data_lifecycle.autopurge_nowriter_samples_delay = new Duration_t();
        localDataReaderQos.reader_data_lifecycle.autopurge_disposed_samples_delay = new Duration_t();

        DataReaderQosHolder localDataReaderQosHolder = new DataReaderQosHolder(localDataReaderQos);
        localSubscriber.get_default_datareader_qos(localDataReaderQosHolder);
        if (reliable) {
            //localDataReaderQosHolder.value.reliability.kind = ReliabilityQosPolicyKind.RELIABLE_RELIABILITY_QOS;
        }
        localDataReaderQosHolder.value.history.kind = HistoryQosPolicyKind.KEEP_ALL_HISTORY_QOS;

        DataReaderListenerImpl localDataReaderListenerImpl = new DataReaderListenerImpl();
        DataReader localDataReader = localSubscriber.create_datareader(localTopic, localDataReaderQosHolder.value, localDataReaderListenerImpl, -1);
        if (localDataReader == null) {
            System.err.println("ERROR: DataReader creation failed");
            return;
        }
        StatusCondition localStatusCondition = localDataReader.get_statuscondition();
        localStatusCondition.set_enabled_statuses(16384);
        WaitSet localWaitSet = new WaitSet();
        localWaitSet.attach_condition(localStatusCondition);
        SubscriptionMatchedStatusHolder localSubscriptionMatchedStatusHolder = new SubscriptionMatchedStatusHolder(new SubscriptionMatchedStatus());
        Duration_t localDuration_t = new Duration_t(Integer.MAX_VALUE, Integer.MAX_VALUE);

        int i = 0;
        for (; ; ) {
            int j = localDataReader.get_subscription_matched_status(localSubscriptionMatchedStatusHolder);
            if (j != 0) {
                System.err.println("ERROR: get_subscription_matched_status()failed.");
                return;
            }
            if ((localSubscriptionMatchedStatusHolder.value.current_count == 0) && (localSubscriptionMatchedStatusHolder.value.total_count > 0)) {
                System.out.println("Subscriber No Longer Matched");
                break;
            }
            if ((localSubscriptionMatchedStatusHolder.value.current_count > 0) && (i == 0)) {
                System.out.println("Subscriber Matched");
                i = 1;
            }
            ConditionSeqHolder localConditionSeqHolder = new ConditionSeqHolder(new Condition[0]);
            if (localWaitSet.wait(localConditionSeqHolder, localDuration_t) != 0) {
                System.err.println("ERROR: wait() failed.");
                return;
            }
        }

        localWaitSet.detach_condition(localStatusCondition);

        System.out.println("Stop Subscriber");

        localDomainParticipant.delete_contained_entities();
        localDomainParticipantFactory.delete_participant(localDomainParticipant);
        TheServiceParticipant.shutdown();

        System.out.println("Subscriber exiting");
    }
}

