package com.ociweb.opendds.subvalve;

import DDS.DataReader;
import DDS.LivelinessChangedStatus;
import DDS.RequestedDeadlineMissedStatus;
import DDS.RequestedIncompatibleQosStatus;
import DDS.SampleInfo;
import DDS.SampleInfoHolder;
import DDS.SampleLostStatus;
import DDS.SampleRejectedStatus;
import DDS.SubscriptionMatchedStatus;
import DDS.Time_t;
import DDS._DataReaderListenerLocalBase;
import Nexmatix.ValveStatus;
import Nexmatix.ValveStatusDataReader;
import Nexmatix.ValveStatusDataReaderHelper;
import Nexmatix.ValveStatusHolder;

import java.util.ArrayList;

public class DataReaderListenerImpl extends _DataReaderListenerLocalBase {
    private ArrayList<Boolean> counts;

    DataReaderListenerImpl() {
        counts = new ArrayList<>(40);
    }

    private void initialize_counts() {
        if (counts.size() > 0) {
            return;
        }
        for (int i = 0; i < 40; i++) {
            counts.add(Boolean.FALSE);
        }
    }

    public synchronized void on_data_available(DataReader paramDataReader) {
        initialize_counts();

        ValveStatusDataReader localValveStatusDataReader = ValveStatusDataReaderHelper.narrow(paramDataReader);
        if (localValveStatusDataReader == null) {
            System.err.println("ERROR: read: narrow failed.");
            return;
        }

        ValveStatusHolder localValveStatusHolder = new ValveStatusHolder(new ValveStatus());
        SampleInfoHolder localSampleInfoHolder = new SampleInfoHolder(new SampleInfo(0, 0, 0, new Time_t(), 0, 0, 0, 0, 0, 0, 0, false, 0L));
        int i = localValveStatusDataReader.take_next_sample(localValveStatusHolder, localSampleInfoHolder);
        if (i == 0) {
            System.out.println("SampleInfo.sample_rank = " + localSampleInfoHolder.value.sample_rank);
            System.out.println("SampleInfo.instance_state = " + localSampleInfoHolder.value.instance_state);
            if (localSampleInfoHolder.value.valid_data) {
                String str = "";
                int j = 0;
                if ((localValveStatusHolder.value.cycleCount < 0) || localValveStatusHolder.value.cycleCount >= counts.size()) {
                    j = 1;
                } else if (!counts.get(localValveStatusHolder.value.cycleCount)) {
                    counts.set(localValveStatusHolder.value.cycleCount, Boolean.TRUE);
                } else {
                    str = "ERROR: Repeat ";
                }
                System.out.println(str + "Status: station = " + localValveStatusHolder.value.stationNumber);
                System.out.println("         timeStamp       = " + localValveStatusHolder.value.timeStamp);
                System.out.println("         cycleCountLimit = " + localValveStatusHolder.value.cycleCountLimit);
                System.out.println("         cycleCount      = " + localValveStatusHolder.value.cycleCount);
                //System.out.println("         pressurePoint   = " + localValveStatusHolder.value.pressurePoint);
                System.out.println("SampleInfo.sample_rank = " + localSampleInfoHolder.value.sample_rank);
                if (j == 1) {
                    System.out.println("ERROR: Invalid message.count (" + localValveStatusHolder.value.cycleCount + ")");
                }
            } else if (localSampleInfoHolder.value.instance_state == 2) {
                System.out.println("instance is disposed");
            } else if (localSampleInfoHolder.value.instance_state == 4) {
                System.out.println("instance is unregistered");
            } else {
                System.out.println("DataReaderListenerImpl::on_data_available: ERROR: received unknown instance state " + localSampleInfoHolder.value.instance_state);
            }
        } else if (i == 11) {
            System.err.println("ERROR: reader received DDS::RETCODE_NO_DATA!");
        } else {
            System.err.println("ERROR: read Message: Error: " + i);
        }
    }

    public void on_requested_deadline_missed(DataReader paramDataReader, RequestedDeadlineMissedStatus paramRequestedDeadlineMissedStatus) {
        System.err.println("DataReaderListenerImpl.on_requested_deadline_missed");
    }

    public void on_requested_incompatible_qos(DataReader paramDataReader, RequestedIncompatibleQosStatus paramRequestedIncompatibleQosStatus) {
        System.err.println("DataReaderListenerImpl.on_requested_incompatible_qos");
    }

    public void on_sample_rejected(DataReader paramDataReader, SampleRejectedStatus paramSampleRejectedStatus) {
        System.err.println("DataReaderListenerImpl.on_sample_rejected");
    }

    public void on_liveliness_changed(DataReader paramDataReader, LivelinessChangedStatus paramLivelinessChangedStatus) {
        System.err.println("DataReaderListenerImpl.on_liveliness_changed");
    }

    public void on_subscription_matched(DataReader paramDataReader, SubscriptionMatchedStatus paramSubscriptionMatchedStatus) {
        System.err.println("DataReaderListenerImpl.on_subscription_matched");
    }

    public void on_sample_lost(DataReader paramDataReader, SampleLostStatus paramSampleLostStatus) {
        System.err.println("DataReaderListenerImpl.on_sample_lost");
    }

    public void report_validity() {
        int j = 0;
        for (Boolean localBoolean : counts) {
            if (!localBoolean) {
                j++;
            }
        }
        if (j > 0) {
            System.out.println("ERROR: Missing " + j + " messages");
        }
    }
}

