export function route(){const [page='overview',id=null,sub=null]=location.hash.replace(/^#\//,'').split('/');return {page,id,sub};}
