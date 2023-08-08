package com.jia.flink.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ClassName: AppEvent
 * Package: com.jia.flink.pojo
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/7 20:02
 * @Version 1.0
 */
@Data
@NoArgsConstructor
@AllArgsConstructor
public class AppEvent {

	private String orderId;

	private String eventType;

	private Long ts;

}
