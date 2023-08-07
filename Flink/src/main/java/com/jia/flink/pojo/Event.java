package com.jia.flink.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ClassName: Event
 * Package: com.jia.flink.pojo
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/7 14:46
 * @Version 1.0
 */

@Data
@NoArgsConstructor
@AllArgsConstructor
public class Event {

	private String user;

	private String url;

	private Long ts;

}
