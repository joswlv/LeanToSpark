package model;

import lombok.Getter;

import java.io.Serializable;

/**
 * spark_test - 2017. 3. 27..
 */
public class DemoInfoKey implements Serializable {
    @Getter
    String uid;
    @Getter String age_code;
    @Getter String gender_code;
    @Getter String marriage;

    public DemoInfoKey(String uid, String age_code, String gender_code, String marriage) {
        this.uid = uid;
        this.age_code = age_code;
        this.gender_code = gender_code;
        this.marriage = marriage;
    }
}
