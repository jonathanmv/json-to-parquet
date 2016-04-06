package jonathanmv.json_to_parquet;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;

import jonathanmv.json_to_parquet.model.FriendsEdge;
import jonathanmv.json_to_parquet.model.PersonID;

public class JsonToThriftConverter {
    private JSONObject relationship;
    private JSONObject personOne;
    private JSONObject personTwo;

    public JsonToThriftConverter(String json) throws ParseException {
        JSONParser parser = new JSONParser();
        this.relationship = (JSONObject) parser.parse(json);
        this.personOne = (JSONObject) this.relationship.get("personOne");
        this.personTwo = (JSONObject) this.relationship.get("personTwo");
    }
  
    public FriendsEdge convert() {
        PersonID one = new PersonID();
        one.setPerson_id(this.personOne.get("id").toString());
        PersonID two = new PersonID();
        two.setPerson_id(this.personTwo.get("id").toString());
        
        FriendsEdge friendsEdge = new FriendsEdge();
        friendsEdge.setOne(one);
        friendsEdge.setTwo(two);
        Long timestamp = Long.parseLong(this.relationship.get("timestamp").toString());
        friendsEdge.setTimestamp(timestamp);
        
        return friendsEdge;
    }
}