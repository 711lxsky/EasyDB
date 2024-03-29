package top.lxsky711.easydb.common.data;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * @Author: 711lxsky
 * @Description:
 */

public class CollectionUtil {

    public static <K, V> boolean judgeElementInListMap(Map<K, List<V>> listMap, K desListIndex, V desElement){
        List<V> desList = listMap.get(desListIndex);
        if(Objects.isNull(desList)){
            return false;
        }
        for (V element : desList){
            if(element.equals(desElement)) {
                return true;
            }
        }
        return false;
    }

    public static <K, V> void putElementIntoListMap(Map<K, List<V>> listMap, K desListIndex, V desElement){
      if(! listMap.containsKey(desListIndex)){
          listMap.put(desListIndex, new ArrayList<>());
      }
      listMap.get(desListIndex).add(desElement);
    }

}
