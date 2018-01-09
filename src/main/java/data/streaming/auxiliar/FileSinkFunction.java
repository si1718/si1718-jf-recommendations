package data.streaming.auxiliar;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.flink.streaming.api.functions.sink.SinkFunction;

import data.streaming.dto.KeywordDTO;

public class FileSinkFunction implements SinkFunction<KeywordDTO> {

	/**
	 * 
	 */
	private static final long serialVersionUID = 1L;

	private static BufferedWriter buff;

	public FileSinkFunction(String path) throws IOException {
		File f = new File(path);
		if (!f.exists() && !f.createNewFile())
			throw new IllegalArgumentException();

		buff = new BufferedWriter(new FileWriter(f));
	}

	public void invoke(KeywordDTO value) throws Exception {
		// Básicamente la idea es construir un map con #h1, #h2, r. Donde r es el número
		// de retuits que tiene un twit que relacionó ambos hashtags.

		if (value != null) {
			buff.write(value.getKey1() + "," + value.getKey2() + "," + value.getStatistic());
			buff.newLine();
			buff.flush();
		}
	}

}
