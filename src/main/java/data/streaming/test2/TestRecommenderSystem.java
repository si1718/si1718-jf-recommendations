package data.streaming.test2;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.bson.Document;
import org.grouplens.lenskit.ItemRecommender;
import org.grouplens.lenskit.RecommenderBuildException;

import com.mongodb.MongoClient;
import com.mongodb.MongoClientURI;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.MongoDatabase;

import data.streaming.dto.KeywordDTO;
import data.streaming.utils.Utils;

public class TestRecommenderSystem {
	
	private static final String PATH = "./out/conferenceRatingData.csv";
	
	private static BufferedWriter buff;
	
	public static void main(String... args) throws IOException, RecommenderBuildException {
		
		MongoClientURI uri = new MongoClientURI("mongodb://jiji:jiji@ds141766.mlab.com:41766/si1718-jf-conferences2");
		MongoClient client = new MongoClient(uri);
		MongoDatabase db = client.getDatabase(uri.getDatabase());
		
		MongoCollection<Document> ratings = db.getCollection("ratings");
		
		//Extraer de la coleccion ratings todos los documentos
		List<Document> ratingDocuments = (List<Document>) ratings.find()
				.into(new ArrayList<Document>());
		
		File f = new File(PATH);
		
		if (f.exists()) {
			f.delete();
		}
		
		if (!f.exists() && !f.createNewFile())
			throw new IllegalArgumentException();

		buff = new BufferedWriter(new FileWriter(f));
		
		for (Document ratDoc:ratingDocuments) {
			if (ratDoc != null) {
				buff.write(ratDoc.getString("idConference1") + "," + ratDoc.getString("idConference2") + "," + ratDoc.getInteger("rating"));
				buff.newLine();
				buff.flush();
			}
		}
		buff.close();
		
		Set<KeywordDTO> set = Utils.getKeywords();
		ItemRecommender irec = Utils.getRecommender(set);
		//Utils.saveModel(irec, set);
		Utils.saveModelInMLab(irec, set);
		System.out.println("Hecho!");
	}
	
}
