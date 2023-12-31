package com.jia.flink.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ClassName: WordCount
 * Package: com.jia.flink.pojo
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/4 10:20
 * @Version 1.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class WordCount {

    private String word;

    private Long count;


}
