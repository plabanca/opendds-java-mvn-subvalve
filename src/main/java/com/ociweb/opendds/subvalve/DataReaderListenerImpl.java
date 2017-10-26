package com.ociweb.opendds.subvalve;

import DDS.*;
import Nexmatix.*;


public class DataReaderListenerImpl extends _DataReaderListenerLocalBase {

    DataReaderListenerImpl() {
    }

    private void printValveData(ValveData valveData) {
        System.out.println("manifoldId:" + valveData.manifoldId);
        System.out.println("stationId:" + valveData.stationId);
        System.out.println("valveSerialId:" + valveData.valveSerialId);
        System.out.println("partNumber:" + valveData.partNumber);
        System.out.println("leakFault:" + valveData.leakFault);
        System.out.println("pressureFault:" + valveData.pressureFault.value());
        System.out.println("cycles:" + valveData.cycles);
        System.out.println("pressure:" + valveData.pressure);
        System.out.println("durationLast12:" + valveData.durationLast12);
        System.out.println("durationLast14:" + valveData.durationLast14);
        System.out.println("equalizationAveragePressure:" + valveData.equalizationAveragePressure);
        System.out.println("residualOfDynamicAnalysis:" + valveData.residualOfDynamicAnalysis);
        System.out.println("suppliedPressure:" + valveData.suppliedPressure);
    }

    private void initValveData(ValveData valveData){
        valveData.manifoldId = 1;
        valveData.stationId  = 2;
        valveData.valveSerialId = 3;
        valveData.partNumber = "1234";
        valveData.leakFault = false;
        valveData.pressureFault = PresureFault.NO_FAULT;
        valveData.cycles = 4;
        valveData.pressure = 5;
        valveData.durationLast12 =6;
        valveData.durationLast14 =7;
        valveData.equalizationAveragePressure =8;
        valveData.residualOfDynamicAnalysis =9;
        valveData.suppliedPressure =10;
    }

    public synchronized void on_data_available(DataReader paramDataReader) {

        System.out.println("on_data_available");

        ValveDataDataReader valveDataDataReader = ValveDataDataReaderHelper.narrow(paramDataReader);
        if (valveDataDataReader == null) {
            System.err.println("ERROR: read: narrow failed.");
            return;
        }

        ValveData valveData = new ValveData();
        initValveData(valveData);
        ValveDataHolder valveDataHolder   = new ValveDataHolder(valveData);
        SampleInfoHolder sampleInfoHolder = new SampleInfoHolder(new SampleInfo(0,0,0,new Time_t(),0,0,0,0,0,0,0,false,0L));
        int take_next_sample_rc = valveDataDataReader.take_next_sample(valveDataHolder, sampleInfoHolder);
        switch (take_next_sample_rc)
        {
            case RETCODE_OK.value:
                System.out.println("SampleInfo.sample_rank = " + sampleInfoHolder.value.sample_rank);
                System.out.println("SampleInfo.instance_state = " + sampleInfoHolder.value.instance_state);
                if (sampleInfoHolder.value.valid_data) {
                    printValveData(valveDataHolder.value);
                } else if (sampleInfoHolder.value.instance_state == NOT_ALIVE_DISPOSED_INSTANCE_STATE.value) {
                    System.out.println("instance is disposed");
                } else if (sampleInfoHolder.value.instance_state == NOT_ALIVE_NO_WRITERS_INSTANCE_STATE.value) {
                    System.out.println("instance is unregistered");
                } else {
                    System.out.println("DataReaderListenerImpl::on_data_available: ERROR: received unknown instance state " + sampleInfoHolder.value.instance_state);
                }
                break;
            case RETCODE_NO_DATA.value:
                System.err.println("ERROR: reader received DDS::RETCODE_NO_DATA!");
                break;
            default:
                System.out.println("take_next_sample_rc:" + take_next_sample_rc);
                break;
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

}

