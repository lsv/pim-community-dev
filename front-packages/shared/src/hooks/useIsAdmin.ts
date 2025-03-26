import {useUserContext} from './useUserContext';

const useIsAdmin = () => {
  const user = useUserContext()
  return user.get('roles').includes('ROLE_ADMINISTRATOR') ?? false
}

export {useIsAdmin};