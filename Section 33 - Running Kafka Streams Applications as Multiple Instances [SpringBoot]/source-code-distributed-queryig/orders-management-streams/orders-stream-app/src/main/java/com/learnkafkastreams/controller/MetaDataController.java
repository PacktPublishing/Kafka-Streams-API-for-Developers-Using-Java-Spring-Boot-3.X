package com.learnkafkastreams.controller;

import com.learnkafkastreams.domain.HostInfoDTO;
import com.learnkafkastreams.service.MetaDataService;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequestMapping("/v1/metadata")
public class MetaDataController {

    private MetaDataService metaDataService;

    public MetaDataController(MetaDataService metaDataService) {
        this.metaDataService = metaDataService;
    }

    @GetMapping("/all")
    public List<HostInfoDTO> getStreamsMetaData(){
        return  metaDataService.getStreamsMetaData();
    }


}
