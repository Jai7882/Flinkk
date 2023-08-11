package com.jia.flink.pojo;

import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * ClassName: UrlViewCount
 * Package: com.jia.flink.pojo
 * Description:
 *
 * @Author jjy
 * @Create 2023/8/10 19:32
 * @Version 1.0
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class UrlViewCount {
	private String url;
	private Long start;
	private Long end;
	private Long count;
}
