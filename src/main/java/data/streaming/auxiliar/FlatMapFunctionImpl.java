package data.streaming.auxiliar;

import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import org.apache.flink.api.common.functions.FlatMapFunction;
import org.apache.flink.util.Collector;

import data.streaming.dto.KeywordDTO;
import data.streaming.dto.TweetDTO;

public class FlatMapFunctionImpl implements FlatMapFunction<TweetDTO, KeywordDTO> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;
	private static final String WHITESPACE = "[ ,.;:\\t\\n]+";

	public void flatMap(TweetDTO arg0, Collector<KeywordDTO> arg1) throws Exception {
		if (arg0.getText() != null) {
			String[] text = arg0.getText().split(WHITESPACE);
			Integer followers = arg0.getUser().getFollowers();

			List<String> list = Arrays.stream(text).filter((String x) -> x.startsWith("#"))
					.collect(Collectors.toList());
			if (list.size() > 1) {
				for (int i = 0; i < list.size(); i++) {
					for (int j = 0; j < list.size(); j++) {
						if (i != j) {
							arg1.collect(new KeywordDTO(list.get(i), list.get(j), followers.doubleValue()));
						}
					}
				}
			}
		}
	}

}
