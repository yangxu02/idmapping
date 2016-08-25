package com.linkx.mapping.context;

/**
 * Created by ulyx.yang@ndpmedia.com on 2014/11/18.
 */
public class Output {

    // event table output
    TableOutput event;

    //
    TableOutput devices;

    //
    TableOutput profiles;

    // alisa
    TableOutput aliases;

    //
    EventCounters counters;

    public TableOutput getAliases() {
        return aliases;
    }

    public void setAliases(TableOutput aliases) {
        this.aliases = aliases;
    }

    public TableOutput getEvent() {
        return event;
    }

    public void setEvent(TableOutput event) {
        this.event = event;
    }

    public TableOutput getDevices() {
        return devices;
    }

    public void setDevices(TableOutput devices) {
        this.devices = devices;
    }

    public TableOutput getProfiles() {
        return profiles;
    }

    public void setProfiles(TableOutput profiles) {
        this.profiles = profiles;
    }

    public EventCounters getCounters() {
        return counters;
    }

    public void setCounters(EventCounters counters) {
        this.counters = counters;
    }
}
