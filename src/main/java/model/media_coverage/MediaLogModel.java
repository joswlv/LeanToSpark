package model.media_coverage;

import lombok.Data;

import java.io.Serializable;

/**
 * Created by Jo_seungwan on 2017. 5. 9..
 */
@Data
public class MediaLogModel implements Serializable {
    private String bid;
    public MediaLogModel(String bid){
        this.bid = bid;
    }

}
