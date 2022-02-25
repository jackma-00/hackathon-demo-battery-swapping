package it.unimore.hackathon.resource;

public interface ResourceDataListener<T> {

    public void onDataChanged(SmartObjectResource<T> resource, T updatedValue);

}
