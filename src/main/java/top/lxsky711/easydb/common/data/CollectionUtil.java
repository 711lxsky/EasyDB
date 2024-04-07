package top.lxsky711.easydb.common.data;

import java.util.*;

/**
 * @Author: 711lxsky
 * @Description: 集合工具类，方法抽象为泛型使用
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

    public static <K, V> void removeElementFromListMap(Map<K, List<V>> listMap, K desListIndex, V desElement){
        List<V> desList = listMap.get(desListIndex);
        if(Objects.isNull(desList)){
            return;
        }
        desList.remove(desElement);
        if(desList.isEmpty()){
            listMap.remove(desListIndex);
        }
    }

    public static <V> boolean judgeElementInList(List<V> list, V desElement){
        for (V element : list){
            if(element.equals(desElement)) {
                return true;
            }
        }
        return false;
    }

    // 泛型方法求并集，结果为List
    public static <T> List<T> getUnionForTwoList(List<T> list1, List<T> list2) {
        Set<T> set = new HashSet<>(list1);
        set.addAll(list2);
        return new ArrayList<>(set);
    }

    // 泛型方法求交集，结果为List
    public static <T> List<T> getIntersectionForTwoList(List<T> list1, List<T> list2) {
        Set<T> set = new HashSet<>(list1);
        set.retainAll(list2);
        return new ArrayList<>(set);
    }

}
