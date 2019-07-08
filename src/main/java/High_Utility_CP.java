import org.apache.hadoop.yarn.webapp.hamlet.Hamlet;
import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.MapFunction;
import org.apache.spark.sql.SparkSession;

import java.math.BigInteger;
import java.util.*;

class Spatial_Feature
{
    private String feature_name;
    private int external_utility;

    public Spatial_Feature(String feature_name, int external_utility) {
        this.feature_name = feature_name;
        this.external_utility = external_utility;
    }

    public String getFeature_name() {
        return feature_name;
    }

    public void setFeature_name(String feature_name) {
        this.feature_name = feature_name;
    }

    public int getExternal_utility() {
        return external_utility;
    }

    public void setExternal_utility(int external_utility) {
        this.external_utility = external_utility;
    }
}
class Spatial_Point
{
    private Spatial_Feature feature_type;
    private int x,y;
//    private String name;

    public Spatial_Point(Spatial_Feature feature_type, int x, int y) {
        this.feature_type = feature_type;
        this.x = x;
        this.y = y;
    }
    public Spatial_Point( int x, int y) {
        this.x = x;
        this.y = y;
    }
    public Spatial_Feature getFeature_type() {
        return feature_type;
    }

    public void setFeature_type(Spatial_Feature feature_type) {
        this.feature_type = feature_type;
    }

    public int getX() {
        return x;
    }

    public void setX(int x) {
        this.x = x;
    }

    public int getY() {
        return y;
    }

    public void setY(int y) {
        this.y = y;
    }
}

public class High_Utility_CP {
    private static boolean find(Spatial_Feature A,Spatial_Feature B,HashMap<Spatial_Point,LinkedList<Spatial_Point>> distance_map)
    {
        for(Map.Entry m:distance_map.entrySet())
        {
            Spatial_Point spatial_point= (Spatial_Point) m.getKey();
            LinkedList<Spatial_Point> lists= (LinkedList<Spatial_Point>) m.getValue();
            if(spatial_point.getFeature_type().getFeature_name().equals(A.getFeature_name()))
            {
                for(int i=0;i<lists.size();i++)
                {
                    if(lists.get(i).getFeature_type().getFeature_name().equals(B.getFeature_name()))
                    {
                        return true;
                    }
                }
            }

        }
        return false;
    }
    private static LinkedList<LinkedList<Spatial_Feature>> gen_neighborhoods(HashMap<Spatial_Point,LinkedList<Spatial_Point>> distance_map,LinkedList<Spatial_Feature> spatial_features)
    {

        LinkedList<LinkedList<Spatial_Feature>> neighbours=new LinkedList<>();
        for(int i=0;i<spatial_features.size();i++)
        {

            for(int j=i+1;j<spatial_features.size();j++)
            {
                LinkedList<Spatial_Feature> list=new LinkedList<>();
                if(find(spatial_features.get(i),spatial_features.get(j),distance_map))
                {
                    list.add(spatial_features.get(i));
                    list.add(spatial_features.get(j));
                    neighbours.add(list);
                }

            }
        }
        return neighbours;
//        return new LinkedList<Spatial_Feature>();
    }

    private static LinkedList<LinkedList<Spatial_Feature>> create_subseq(LinkedList<Spatial_Feature> spatial_features,int k)
    {
        LinkedList<LinkedList<Spatial_Feature>> candidate=new LinkedList<>();
        int opsize= (int) Math.pow(2,spatial_features.size());
        for(int c=1;c<opsize;c++)
        {
            LinkedList<Spatial_Feature> list=new LinkedList<>();
            for(int j=0;j<spatial_features.size();j++)
            {
                if(BigInteger.valueOf(c).testBit(j))
                {
                    list.add(spatial_features.get(j));
                }
            }
            if(list.size()==k)
            {
                candidate.add(list);
            }
        }
        return candidate;
    }
    private static LinkedList<LinkedList<Spatial_Feature>> create_subseq_top_s(LinkedList<Spatial_Feature> spatial_features,int s,HashMap<String,Integer> map)
    {
        LinkedList<LinkedList<Spatial_Feature>> candidate=new LinkedList<>();
        LinkedList<String> list=new LinkedList<>();
        int opsize= (int) Math.pow(2,spatial_features.size());
        for(int c=1;c<opsize;c++)
        {
//            LinkedList<Spatial_Feature> list=new LinkedList<>();
            StringBuilder str=new StringBuilder();
            for(int j=0;j<spatial_features.size();j++)
            {
                if(BigInteger.valueOf(c).testBit(j))
                {
                    str.append(spatial_features.get(j).getFeature_name());
                }
            }
            list.add(str.toString());
        }
        Collections.sort(list, (o1, o2) -> o2.length()-o1.length());

        for(int i=0;i<list.size();i++)
        {
            for(int j=i+1;j<list.size();j++)
            {
                if(list.get(i).compareTo(list.get(j))>0)
                {
                    StringBuilder val=new StringBuilder(list.get(i));
                    list.set(i,list.get(j));
                    list.set(j,val.toString());
                }
                else
                {
                    break;
                }
            }
        }
        for(int i=0;i<s;i++)
        {
            LinkedList<Spatial_Feature> top_s_list=new LinkedList<>();
            String[] arr=list.get(i).split("");
            for(int j=0;j<arr.length;j++)
            {
                Spatial_Feature f=new Spatial_Feature(arr[j],(int)map.get(arr[j]));
                top_s_list.add(f);
            }

            candidate.add(top_s_list);
        }
        return candidate;
    }
    private static LinkedList<LinkedList<Spatial_Feature>> gen_candidate_colocations(LinkedList<LinkedList<Spatial_Feature>> colocation_n,LinkedList<Spatial_Feature> spatial_features,int k)
    {


        LinkedList<LinkedList<Spatial_Feature>> candidate_features=create_subseq(spatial_features,k);
//        for(int i=0;i<colocation_n.size();i++)
//        {
//            for(int j=0;j<colocation_n.get(i).size();j++)
//            {
//                System.out.print(colocation_n.get(i).get(j).getFeature_name()+" ");
//            }
//            System.out.println();
//        }
        HashSet<Integer> rem=new HashSet<>();
        for(int i=0;i<candidate_features.size();i++)
        {

            //list of all subseq of list in candidate features.
            LinkedList<LinkedList<Spatial_Feature>> list=create_subseq(candidate_features.get(i),k-1);
            int all_found=0;
            int found=0;

            int x=0;
            for(int j=0;j<list.size();j++)
            {
                int f=0;
                for(int m=0;m<colocation_n.size();m++)
                {
                    int count=0;
                    for(int l=0;l<list.get(j).size();l++)
                    {
                        String val=list.get(j).get(l).getFeature_name();
                        int flag=0;
                        for(int n=0;n<colocation_n.get(m).size();n++)
                        {
                            if(colocation_n.get(m).get(n).getFeature_name().equals(val))
                            {
                                flag=1;
                                break;
                            }
                        }
                        if(flag==1)
                        {
                            count++;
                        }

                    }
                    if(count==list.get(j).size())
                    {
                        f=1;
                       found++;
                       break;
                    }
                }
                if(f==0)
                {
                    break;
                }
                if(found==list.size())
                {
                    all_found=1;
                    break;
                }
//

            }
            if(all_found==0)
            {
                rem.add(i);
            }
//
        }
        LinkedList<LinkedList<Spatial_Feature>> final_out=new LinkedList<>();
        for(int i=0;i<candidate_features.size();i++)
        {
            if(!rem.contains(i)) {
                final_out.add(candidate_features.get(i));
            }
        }
//        System.out.println(final_out.size());
//        for(int i=0;i<final_out.size();i++)
//        {
//            for(int j=0;j<final_out.get(i).size();j++)
//            {
//                System.out.print(final_out.get(i).get(j).getFeature_name()+" ");
//            }
//            System.out.println();
//        }
        return final_out;
    }
    private static double calca_dist(Spatial_Point s1,Spatial_Point s2)
    {
        double diff_x=Math.pow(s1.getX()-s2.getX(),2);
        double diff_y=Math.pow(s1.getY()-s2.getY(),2);

        return Math.sqrt(diff_x+diff_y);
    }
    public static void main(String args[])
    {
//        SparkSession spark= SparkSession.builder()
//                .master("local")
//                .appName("BTP")
//                .getOrCreate();
        final double pattern_utility_ratio_threshold=0.25;
        Random random=new Random();
        // adding point to the list
        LinkedList<Spatial_Point> points_lists=new LinkedList<Spatial_Point>();
        for(int i=0;i<20;i++)
        {
            int x=random.nextInt(100);
            int y=random.nextInt(100);
            points_lists.add(new Spatial_Point(x,y));
        }
        //adding spatial feature to a list
        String[] arr={"A","B","C","D","E","F"};
        HashMap<String,Integer> map=new HashMap<String, Integer>();
        LinkedList<Spatial_Feature> spatial_features=new LinkedList<Spatial_Feature>();
        for(int i=0;i<arr.length;i++)
        {
            int val=random.nextInt(100);
            map.put(arr[i],val);
            Spatial_Feature f=new Spatial_Feature(arr[i],val);
            spatial_features.add(f);
        }
//        for(int i=0;i<spatial_features.size();i++)
//        {
//            System.out.println(spatial_features.get(i).getFeature_name()+" "+spatial_features.get(i).getExternal_utility());
//        }

        //assigning feature_type to the spatial point

        for(int i=0;i<points_lists.size();i++)
        {
            int ind=random.nextInt(spatial_features.size());

            points_lists.get(i).setFeature_type(spatial_features.get(ind));
            System.out.println(points_lists.get(i).getFeature_type().getFeature_name()+" "+points_lists.get(i).getX()+" "+points_lists.get(i).getY());
        }


        //counting the instances of each feature
        int utility=0;
        HashMap<String,Integer> count_instances=new HashMap<String, Integer>();//count of instances of feature instances
        for(int i=0;i<points_lists.size();i++)
        {

            if(count_instances.containsKey(points_lists.get(i).getFeature_type().getFeature_name()))
            {
                count_instances.replace(points_lists.get(i).getFeature_type().getFeature_name(),count_instances.get(points_lists.get(i).getFeature_type().getFeature_name()),count_instances.get(points_lists.get(i).getFeature_type().getFeature_name())+1);
            }
            else
            {
                count_instances.put(points_lists.get(i).getFeature_type().getFeature_name(),1);
            }
        }

//        for(Map.Entry m:count_instances.entrySet())
//        {
//            System.out.println(m.getKey()+" "+m.getValue());
//        }

        //calculating database utility
        for(Map.Entry m:count_instances.entrySet())
        {
            utility+=(int)m.getValue()*map.get(m.getKey());

        }

        final int database_utility=utility;//cannot change the database utility
        System.out.println(database_utility);
        double threshold_dist=32;//random

        //making the table instances or the points whose distance to the query point is less than threshold
        //making points connected to a certain point
        HashMap<Spatial_Point,LinkedList<Spatial_Point>> distance_map=new HashMap<>();
        for(int i=0;i<points_lists.size();i++)
        {
            LinkedList<Spatial_Point> near_points=new LinkedList<>();
            for(int j=0;j<points_lists.size();j++)
            {
                if(i!=j) {
                    if (!points_lists.get(i).getFeature_type().getFeature_name().equals(points_lists.get(j).getFeature_type().getFeature_name())) {
                        if (calca_dist(points_lists.get(i), points_lists.get(j)) <= threshold_dist) {
                            near_points.add(points_lists.get(j));
                        }
                    }
                }
            }
            distance_map.put(points_lists.get(i),near_points);
        }


        for(Map.Entry m:distance_map.entrySet())
        {
            Spatial_Point spatial_point= (Spatial_Point) m.getKey();
            LinkedList<Spatial_Point> lists= (LinkedList<Spatial_Point>) m.getValue();
            System.out.print(spatial_point.getFeature_type().getFeature_name()+" "+spatial_point.getX()+" "+spatial_point.getY()+"=>");
            for(int i=0;i<lists.size();i++)
            {
                System.out.print(lists.get(i).getFeature_type().getFeature_name()+" "+lists.get(i).getX()+" "+lists.get(i).getY()+",");
            }
            System.out.println();
        }





        int num_of_features=spatial_features.size();//total number of features

        LinkedList<LinkedList<LinkedList<Spatial_Feature>>>high_utility_colocation=new LinkedList<>();//final output
        LinkedList<LinkedList<LinkedList<Spatial_Feature>>> candidate_colocations=new LinkedList<>();//candidate colocations

//        for(int i=2;i<=num_of_features;i++)
//        {
//            LinkedList<Spatial_Feature> list=new LinkedList<>();
//            high_utility_colocation.add(list);
//            list=new LinkedList<>();
//            candidate_colocations.add(list);
//        }
        int k=2;
        while(k<=num_of_features)
        {
            int ind=k-2;
            if(k==2)
            {
                LinkedList<LinkedList<Spatial_Feature>> list=gen_neighborhoods(distance_map,spatial_features);
                candidate_colocations.add(list);
                for(int i=0;i<list.size();i++)
                {
                    for(int j=0;j<list.get(i).size();j++)
                    {
                        System.out.print(list.get(i).get(j).getFeature_name()+" ");
                    }
                    System.out.println();
                }
            }
            else
            {
                LinkedList<LinkedList<Spatial_Feature>> list=gen_candidate_colocations(candidate_colocations.get(ind-1),spatial_features,k);
                candidate_colocations.add(list);
                for(int i=0;i<list.size();i++)
                {
                    for(int j=0;j<list.get(i).size();j++)
                    {
                        System.out.print(list.get(i).get(j).getFeature_name()+" ");
                    }
                    System.out.println();
                }
            }
//            LinkedList<Spatial_Feature> candidate_cl_k=candidate_colocations.get(ind);
            LinkedList<LinkedList<Spatial_Feature>> coloc=candidate_colocations.get(ind);//set of colocations of size k
            LinkedList<LinkedList<Spatial_Feature>> utility_coloc=new LinkedList<>();//list for high utility colocations of size k
            for(int i=0;i<coloc.size();i++)
            {
                LinkedList<Spatial_Feature> list=coloc.get(i);//colocation of size k
                HashSet<Spatial_Point> points_set=new HashSet<>();
                if(list.size()==2)
                {
                    for(Map.Entry m:distance_map.entrySet())
                    {
                        Spatial_Point key= (Spatial_Point) m.getKey();
                        if(key.getFeature_type().getFeature_name().equals(list.get(0).getFeature_name()))
                        {
                            LinkedList<Spatial_Point> point= (LinkedList<Spatial_Point>) m.getValue();
                            for(int j=0;j<point.size();j++)
                            {
                                if(point.get(j).getFeature_type().getFeature_name().equals(list.get(1).getFeature_name()))
                                {
                                    points_set.add(point.get(j));
                                    points_set.add(key);
                                }
                            }
                        }
                    }
//                    System.out.println("hashset");
//                    Iterator itr=points_set.iterator();
//                    while(itr.hasNext())
//                    {
//                        Spatial_Point p= (Spatial_Point) itr.next();
//                        System.out.println(p.getFeature_type().getFeature_name()+" "+p.getX()+" "+p.getY());
//                    }
                }
                else
                {
                    for(Map.Entry m:distance_map.entrySet())
                    {
                        LinkedList<Spatial_Feature> feature=new LinkedList<>();
                        for(int j=0;j<list.size();j++)
                        {
                            feature.add(list.get(j));
                        }
                        Spatial_Point key= (Spatial_Point) m.getKey();
                        if(feature.contains(key.getFeature_type()))
                        {
                            feature.remove(key.getFeature_type());
                            LinkedList<Spatial_Point> point= (LinkedList<Spatial_Point>) m.getValue();
//                            for(int j=0;j<feature.size();j++)
//                            {
                                for(int q=0;q<point.size();q++)
                                {
                                    if(feature.contains(point.get(q).getFeature_type()))
                                    {
                                        feature.remove(point.get(q).getFeature_type());
                                    }
                                }

//                            }
                            if(feature.size()==0)
                            {
                                feature=new LinkedList<>();
                                for(int j=0;j<list.size();j++)
                                {
                                    feature.add(list.get(j));
                                }
                                points_set.add(key);
                                feature.remove(key);
                                for(int j=0;j<point.size();j++)
                                {
                                    if(feature.contains(point.get(j).getFeature_type()))
                                    {
                                        points_set.add(point.get(j));
                                    }
                                }

                            }

                        }


                    }
                }
                HashMap<Spatial_Feature,Integer> table_instance=new HashMap<>();
                Iterator itr=points_set.iterator();
                while(itr.hasNext())
                {
                    Spatial_Point f= (Spatial_Point) itr.next();
                    if(table_instance.containsKey(f.getFeature_type()))
                    {
                        table_instance.replace(f.getFeature_type(),table_instance.get(f.getFeature_type()),table_instance.get(f.getFeature_type())+1);
                    }
                    else
                    {
                        table_instance.put(f.getFeature_type(),1);
                    }
                }

                int pattern_utility=0;
                for(Map.Entry m:table_instance.entrySet())
                {
                    Spatial_Feature f=(Spatial_Feature)m.getKey();
                    int val= ((int) m.getValue())*(f.getExternal_utility());
                    pattern_utility+=val;
                }

                //lamda(c) for the colocation
                double lambda_colocation=((double)pattern_utility)/database_utility;
                System.out.println(lambda_colocation);
                if(lambda_colocation>pattern_utility_ratio_threshold)
                {
                    utility_coloc.add(list);
                }
                else
                {
                    LinkedList<LinkedList<Spatial_Feature>> top_s_subset_of_coloc=create_subseq_top_s(list,list.size(),map);
                    LinkedList<HashSet<Spatial_Point>> single_feature_table_instance=new LinkedList<>();
                    for(int j=0;j<top_s_subset_of_coloc.size();j++)
                    {
                        LinkedList<Spatial_Feature> coloc_list_for_vss=top_s_subset_of_coloc.get(j);
                        HashSet<Spatial_Point> vss_set=new HashSet<>();

                        if(coloc_list_for_vss.size()==2)
                        {
                            for(Map.Entry m:distance_map.entrySet())
                            {
                                Spatial_Point key= (Spatial_Point) m.getKey();
                                if(key.getFeature_type().getFeature_name().equals(coloc_list_for_vss.get(0).getFeature_name()))
                                {
                                    LinkedList<Spatial_Point> point= (LinkedList<Spatial_Point>) m.getValue();
                                    for(int l=0;l<point.size();l++)
                                    {
                                        if(point.get(l).getFeature_type().getFeature_name().equals(list.get(1).getFeature_name()))
                                        {
                                            vss_set.add(point.get(l));
                                            vss_set.add(key);
                                        }
                                    }
                                }
                            }
                        }
                        else
                        {
                            for(Map.Entry m:distance_map.entrySet())
                            {
                                LinkedList<Spatial_Feature> feature=new LinkedList<>();
                                for(int l=0;l<coloc_list_for_vss.size();l++)
                                {
                                    feature.add(coloc_list_for_vss.get(l));
                                }
                                Spatial_Point key= (Spatial_Point) m.getKey();
                                if(feature.contains(key.getFeature_type()))
                                {
                                    feature.remove(key.getFeature_type());
                                    LinkedList<Spatial_Point> point= (LinkedList<Spatial_Point>) m.getValue();
//                            for(int j=0;j<feature.size();j++)
//                            {
                                    for(int q=0;q<point.size();q++)
                                    {
                                        if(feature.contains(point.get(q).getFeature_type()))
                                        {
                                            feature.remove(point.get(q).getFeature_type());
                                        }
                                    }

//                            }
                                    if(feature.size()==0)
                                    {
                                        feature=new LinkedList<>();
                                        for(int l=0;l<list.size();l++)
                                        {
                                            feature.add(list.get(l));
                                        }
                                        vss_set.add(key);
                                        feature.remove(key);
                                        for(int l=0;l<point.size();l++)
                                        {
                                            if(feature.contains(point.get(l).getFeature_type()))
                                            {
                                                vss_set.add(point.get(l));
                                            }
                                        }

                                    }

                                }


                            }
                        }
                        single_feature_table_instance.add(vss_set);


                    }
                    HashSet<Spatial_Point> final_point_vss=new HashSet<>();
                    HashSet<Spatial_Point> hashSet_distinct_points_in_all_subsets=single_feature_table_instance.get(0);
                    Iterator iterator=hashSet_distinct_points_in_all_subsets.iterator();
                    while(iterator.hasNext())
                    {
                        Spatial_Point e= (Spatial_Point) iterator.next();
                        int flag=1;
                        for(int j=1;j<single_feature_table_instance.size();j++)
                        {
                            if(!single_feature_table_instance.get(j).contains(e))
                            {
                                flag=0;
                                break;
                            }
                        }
                        if(flag==1)
                        {
                            final_point_vss.add(e);
                        }
                    }
                    HashMap<Spatial_Feature,Integer> table_instance_vss=new HashMap<>();
                    itr=final_point_vss.iterator();
                    while(itr.hasNext())
                    {
                        Spatial_Point f= (Spatial_Point) itr.next();
                        if(table_instance_vss.containsKey(f.getFeature_type()))
                        {
                            table_instance_vss.replace(f.getFeature_type(),table_instance_vss.get(f.getFeature_type()),table_instance_vss.get(f.getFeature_type())+1);
                        }
                        else
                        {
                            table_instance_vss.put(f.getFeature_type(),1);
                        }
                    }
                    int vss=0;
                    for(Map.Entry m:table_instance_vss.entrySet())
                    {
                        Spatial_Feature f=(Spatial_Feature)m.getKey();
                        int val= ((int) m.getValue())*(f.getExternal_utility());
                        vss+=val;
                    }
                    double EPUR_coloc=lambda_colocation+(((double)vss)/database_utility);
                    if(EPUR_coloc >= pattern_utility_ratio_threshold)
                    {
                        utility_coloc.add(list);
                    }

                }


            }
            candidate_colocations.set(ind,utility_coloc);
            high_utility_colocation.add(utility_coloc);

            k++;
        }
        System.out.println("Final high utility colocations");
        for(int i=0;i<high_utility_colocation.size();i++)
        {
            for(int j=0;j<high_utility_colocation.get(i).size();j++)
            {
                for(int l=0;l<high_utility_colocation.get(i).get(j).size();l++)
                {
                    System.out.print(high_utility_colocation.get(i).get(j).get(l).getFeature_name()+" ");
                }
                System.out.println();
            }
            System.out.println();
        }
    }
}
