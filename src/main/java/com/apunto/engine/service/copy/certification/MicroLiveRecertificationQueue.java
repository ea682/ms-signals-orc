package com.apunto.engine.service.copy.certification;

import com.apunto.engine.entity.MicroLiveRecertificationRequestEntity;

public interface MicroLiveRecertificationQueue {

    MicroLiveRecertificationRequestEntity enqueue(MicroLiveRecertificationRequest request);
}
