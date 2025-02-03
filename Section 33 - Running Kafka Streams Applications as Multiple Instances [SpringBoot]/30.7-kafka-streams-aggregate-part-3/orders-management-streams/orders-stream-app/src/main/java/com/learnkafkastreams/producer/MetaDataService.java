package com.learnkafkastreams.producer;

import com.learnkafkastreams.domain.HostInfoDTO;
import org.springframework.kafka.config.StreamsBuilderFactoryBean;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.stream.Collectors;

@Service
public class MetaDataService {

    private final StreamsBuilderFactoryBean streamsBuilderFactoryBean;


    public MetaDataService(StreamsBuilderFactoryBean streamsBuilderFactoryBean) {
        this.streamsBuilderFactoryBean = streamsBuilderFactoryBean;
    }

    public List<HostInfoDTO> getStreamsMetaData(){

        return streamsBuilderFactoryBean
                .getKafkaStreams()
                .metadataForAllStreamsClients()
                .stream()
                .map(streamsMetadata -> {
                    var hostInfo = streamsMetadata.hostInfo();
                    return new HostInfoDTO(hostInfo.host(),hostInfo.port());
                })
                .collect(Collectors.toList());
    }
}
